/*
 *  Copyright (C) 2025 Michael Bachmann
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use miette::{Context, IntoDiagnostic, Result};
use std::{fmt, ops::ControlFlow, process::Stdio, time::Duration};
use tokio::{
    process::{Child, ChildStdin, Command},
    select,
    sync::mpsc,
    time::{Interval, interval, sleep},
};
use tokio_process_terminate::TerminateExt;
use tosub::SubsystemHandle;
use tracing::{info, instrument, warn};

#[derive(Debug, Clone)]
pub struct CommandDefinition {
    cmd: String,
    args: Vec<String>,
}

impl CommandDefinition {
    pub fn new(cmd: String, args: Vec<String>) -> Self {
        Self { cmd, args }
    }
}

impl From<&CommandDefinition> for Command {
    fn from(value: &CommandDefinition) -> Self {
        let mut cmd = Command::new(&value.cmd);
        for arg in &value.args {
            cmd.arg(arg);
        }
        cmd.stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .stdin(Stdio::piped());
        cmd
    }
}

impl fmt::Display for CommandDefinition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.cmd, self.args.join(" "))
    }
}

pub struct ChildProcessManagerActor {
    subsys: SubsystemHandle,
    api_rx: mpsc::Receiver<ChildProcessMessage>,
    command: Option<CommandDefinition>,
    process: Option<(Child, String)>,
    stdin: Option<ChildStdin>,
    started: bool,
    stopped: bool,
    restart: bool,
}

pub struct ChildProcessManager {
    api_tx: mpsc::Sender<ChildProcessMessage>,
    subsys: SubsystemHandle,
}

impl Drop for ChildProcessManager {
    fn drop(&mut self) {
        self.subsys.request_local_shutdown();
    }
}

#[derive(Debug)]
enum ChildProcessMessage {
    Restart(CommandDefinition),
}

impl ChildProcessManagerActor {
    async fn run_process_manager(mut self) -> Result<()> {
        let mut crash_counter = 0;
        let mut interval = interval(Duration::from_secs(10));
        let mut wait = None;

        while !self.stopped {
            if let ControlFlow::Break(()) = self
                .run_child_process(&mut crash_counter, &mut interval, &mut wait)
                .await?
            {
                break;
            }
        }

        self.child_process_stopped();

        Ok(())
    }

    #[instrument(skip(self), fields())]
    fn child_process_stopped(self) {
        info!("Child process manager actor stopped.");

        if !self.restart && self.started {
            info!("Automatic restart disabled, shutting down …");
            self.subsys.request_global_shutdown();
        }
    }

    async fn run_child_process(
        &mut self,
        crash_counter: &mut usize,
        interval: &mut Interval,
        wait: &mut Option<u64>,
    ) -> Result<ControlFlow<()>> {
        if let Some(command) = self.command.clone() {
            if let Some((proc, cmd)) = self.process.take() {
                return self
                    .monitor_process(proc, cmd, crash_counter, wait, interval)
                    .await;
            } else {
                self.trigger_restart(command)?;
            }
        } else {
            select! {
                recv = self.api_rx.recv() => if let Some(msg) = recv {
                    self.process_msg(msg).await?;
                },
                _ = self.subsys.shutdown_requested() => self.stop().await?,
            }
        }

        if let Some(millis) = wait.take() {
            self.wait(millis).await?;
        }

        Ok(ControlFlow::Continue(()))
    }

    #[instrument(skip(self), fields())]
    async fn wait(&mut self, millis: u64) -> Result<()> {
        select! {
            _ = sleep(Duration::from_millis(millis)) => (),
            _ = self.subsys.shutdown_requested() => self.stop().await?,
        }

        Ok(())
    }

    #[instrument(
        skip(self),
        fields(%command)
    )]
    fn trigger_restart(&mut self, command: CommandDefinition) -> Result<()> {
        info!("(Re-)starting child daemon process {} …", command.cmd);
        let cmd = command.cmd.to_owned();
        let mut command = Command::from(&command);
        let mut proc = command
            .spawn()
            .into_diagnostic()
            .wrap_err_with(|| format!("could not start child process {cmd}"))?;
        self.stdin = proc.stdin.take();
        self.process = Some((proc, cmd));
        self.started = true;
        Ok(())
    }

    async fn monitor_process(
        &mut self,
        mut proc: Child,
        cmd: String,
        crash_counter: &mut usize,
        wait: &mut Option<u64>,
        interval: &mut Interval,
    ) -> Result<ControlFlow<()>> {
        select! {
            recv = self.api_rx.recv() => if let Some(msg) = recv {
                self.process = Some((proc, cmd));
                self.process_msg(msg).await?;
            } else {
                return Ok(ControlFlow::Break(()));
            },
            exit_code = proc.wait() => {
                match exit_code.into_diagnostic().wrap_err_with(||format!("could not get exit code of child process {cmd}"))?.code() {
                    Some(exit_code) => warn!("Child process {} terminated with exit code {exit_code}.", cmd),
                    None => warn!("Child process {} terminated with unknown exit code", cmd)
                }
                if *crash_counter >= 10 {
                    self.restart = false;
                }
                if self.restart {
                    *crash_counter += 1;
                    let del = delay(*crash_counter);
                    *wait = Some(del);
                    info!("Restarting (crashed {crash_counter} time(s), restart delay {del}ms) …");
                } else {
                    self.stop().await?;
                }
            },
            _ = interval.tick() => {
                self.process = Some((proc, cmd));
                if *crash_counter > 0 {
                    *crash_counter -= 1;
                }
            },
            _ = self.subsys.shutdown_requested() => {
                self.process = Some((proc, cmd));
                self.stop().await?;
            },
        }

        Ok(ControlFlow::Continue(()))
    }

    #[instrument(skip(self), err)]
    async fn process_msg(&mut self, msg: ChildProcessMessage) -> Result<()> {
        match msg {
            ChildProcessMessage::Restart(command) => self.restart(command).await?,
        }
        Ok(())
    }

    #[instrument(skip(self), err)]
    async fn restart(&mut self, command: CommandDefinition) -> Result<()> {
        self.command = Some(command);
        if let Some((mut proc, cmd)) = self.process.take() {
            terminate(&mut proc, &cmd).await?;
        }
        Ok(())
    }

    #[instrument(skip(self), err)]
    async fn stop(&mut self) -> Result<()> {
        info!("Stopping child daemon process …");
        self.stopped = true;
        if let Some((mut proc, cmd)) = self.process.take() {
            terminate(&mut proc, &cmd).await?;
            proc.wait()
                .await
                .into_diagnostic()
                .wrap_err_with(|| format!("error waiting for child process {cmd} to stop"))?;
        } else {
            warn!("Process was not running, nothing to stop.");
        }
        Ok(())
    }
}

impl ChildProcessManager {
    pub fn new(subsys: &SubsystemHandle, name: &str, restart: bool) -> Self {
        let (api_tx, api_rx) = mpsc::channel(1);

        let subsys = subsys.spawn(name, async move |s| {
            let actor = ChildProcessManagerActor {
                subsys: s,
                api_rx,
                process: None,
                stdin: None,
                command: None,
                started: false,
                stopped: false,
                restart,
            };
            actor.run_process_manager().await
        });

        ChildProcessManager { api_tx, subsys }
    }

    #[instrument(skip(self), fields())]
    pub async fn restart(&mut self, command: CommandDefinition) {
        self.api_tx
            .send(ChildProcessMessage::Restart(command))
            .await
            .ok();
    }

    #[instrument(skip(self), err)]
    pub async fn stop(&mut self) -> Result<()> {
        self.subsys.request_local_shutdown();
        self.subsys.join().await;
        Ok(())
    }
}

#[instrument(skip(proc), err)]
async fn terminate(proc: &mut Child, cmd: &str) -> Result<()> {
    info!("Terminating child process {cmd} …");
    match proc
        .terminate_wait()
        .await
        .into_diagnostic()
        .wrap_err_with(|| format!("error waiting for child process {cmd} to stop"))?
        .code()
    {
        Some(exit_code) => info!("Child process terminated with exit code {exit_code}."),
        None => warn!("Child process did not terminate cleanly."),
    }
    Ok(())
}

fn delay(crash_counter: usize) -> u64 {
    ((crash_counter as f32).powi(2) * 50.0) as u64
}

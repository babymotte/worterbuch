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
use std::{error::Error, process::Stdio, time::Duration};
use tokio::{
    process::{Child, ChildStdin, Command},
    select,
    sync::mpsc,
    time::{interval, sleep},
};
use tokio_graceful_shutdown::{NestedSubsystem, SubsystemBuilder, SubsystemHandle};
use tokio_process_terminate::TerminateExt;
use tracing::{info, warn};

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
    subsys: NestedSubsystem<Box<(dyn Error + Send + Sync + 'static)>>,
}

impl Drop for ChildProcessManager {
    fn drop(&mut self) {
        self.subsys.initiate_shutdown();
    }
}

enum ChildProcessMessage {
    Restart(CommandDefinition),
}

impl ChildProcessManagerActor {
    async fn run(mut self) -> Result<()> {
        let mut crash_counter = 0;
        let mut interval = interval(Duration::from_secs(10));
        let mut wait = None;

        while !self.stopped {
            if let Some(command) = &self.command {
                if let Some((mut proc, cmd)) = self.process.take() {
                    select! {
                        recv = self.api_rx.recv() => if let Some(msg) = recv {
                            self.process = Some((proc, cmd));
                            self.process_msg(msg).await?;
                        } else {
                            break;
                        },
                        exit_code = proc.wait() => {
                            match exit_code.into_diagnostic().wrap_err_with(||format!("could not get exit code of child process {}", cmd))?.code() {
                                Some(exit_code) => warn!("Child process {} terminated with exit code {exit_code}.", cmd),
                                None => warn!("Child process {} terminated with unknown exit code", cmd)
                            }
                            if self.restart {
                                info!("Restarting …");
                                crash_counter += 1;
                                wait = Some(delay(crash_counter));
                            } else {
                                self.stop().await?;
                            }
                        },
                        _ = interval.tick() => {
                            self.process = Some((proc, cmd));
                            crash_counter = crash_counter.saturating_sub(1);
                        },
                        _ = self.subsys.on_shutdown_requested() => {
                            self.process = Some((proc, cmd));
                            self.stop().await?;
                        },
                    }
                } else {
                    info!("(Re-)starting child daemon process {} …", command.cmd);
                    let cmd = command.cmd.to_owned();
                    let mut command = Command::from(command);
                    let mut proc = command
                        .spawn()
                        .into_diagnostic()
                        .wrap_err_with(|| format!("could not start child process {}", cmd))?;
                    self.stdin = proc.stdin.take();
                    self.process = Some((proc, cmd));
                    self.started = true;
                }
            } else {
                select! {
                    recv = self.api_rx.recv() => if let Some(msg) = recv {
                        self.process_msg(msg).await?;
                    },
                    _ = self.subsys.on_shutdown_requested() => self.stop().await?,
                }
            }

            if let Some(millis) = wait {
                wait = None;
                select! {
                    _ = sleep(Duration::from_millis(millis)) => (),
                    _ = self.subsys.on_shutdown_requested() => self.stop().await?,
                }
            }
        }

        info!("Child process manager actor stopped.");

        if !self.restart && self.started {
            info!("Automatic restart disabled, shutting down …");
            self.subsys.request_shutdown();
        }

        Ok(())
    }

    async fn process_msg(&mut self, msg: ChildProcessMessage) -> Result<()> {
        match msg {
            ChildProcessMessage::Restart(command) => self.restart(command).await?,
        }
        Ok(())
    }

    async fn restart(&mut self, command: CommandDefinition) -> Result<()> {
        self.command = Some(command);
        if let Some((mut proc, cmd)) = self.process.take() {
            terminate(&mut proc, &cmd).await?;
        }
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        info!("Stopping child daemon process …");
        self.stopped = true;
        if let Some((mut proc, cmd)) = self.process.take() {
            terminate(&mut proc, &cmd).await?;
            proc.wait()
                .await
                .into_diagnostic()
                .wrap_err_with(|| format!("error waiting for child process {} to stop", cmd))?;
        } else {
            warn!("Process was not running, nothing to stop.");
        }
        Ok(())
    }
}

impl ChildProcessManager {
    pub fn new(subsys: &SubsystemHandle, name: impl AsRef<str>, restart: bool) -> Self {
        let (api_tx, api_rx) = mpsc::channel(1);

        let subsys = subsys.start(SubsystemBuilder::new(name.as_ref(), move |s| async move {
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
            actor.run().await
        }));

        ChildProcessManager { api_tx, subsys }
    }

    pub async fn restart(&mut self, command: CommandDefinition) {
        self.api_tx
            .send(ChildProcessMessage::Restart(command))
            .await
            .ok();
    }

    pub async fn stop(&self) -> Result<()> {
        self.subsys.initiate_shutdown();
        self.subsys
            .join()
            .await
            .into_diagnostic()
            .wrap_err("error waiting for subsystem to stop")?;
        Ok(())
    }
}

async fn terminate(proc: &mut Child, cmd: &str) -> Result<()> {
    info!("Terminating child process {cmd} …");
    match proc
        .terminate_wait()
        .await
        .into_diagnostic()
        .wrap_err_with(|| format!("error waiting for child process {} to stop", cmd))?
        .code()
    {
        Some(exit_code) => info!("Child process terminated with exit code {exit_code}."),
        None => warn!("Child process did not terminate cleanly."),
    }
    Ok(())
}

fn delay(crash_counter: usize) -> u64 {
    (((crash_counter as f32).ln() * 500.0 + 1.0).round()) as u64
}

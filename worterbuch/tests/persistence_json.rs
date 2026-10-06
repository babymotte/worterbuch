/*
 *  Copyright (C) 2024 Michael Bachmann
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

mod test_common;

use crate::test_common::TestRunner;

#[test]
fn grave_goods_and_last_will_are_presisted_with_json_storage_and_applied_after_crash() {
    let runner = TestRunner::new(
        "./tests/json/server_setup_json.json",
        "./tests/json/test_grave_goods_last_will.json",
    );
    let active_runner = runner.start_wb(true);
    let (mut client1, tests1) = active_runner.start_client_test();
    let (mut client2, tests2) = active_runner.start_client_test();
    let (mut client3, tests3) = active_runner.start_client_test();
    active_runner.run_test(&mut client1, &tests1[0]);
    active_runner.run_test(&mut client2, &tests2[0]);
    active_runner.run_test(&mut client3, &tests3[0]);
    // wait for persistence interval to elapse
    std::thread::sleep(std::time::Duration::from_millis(1100));
    let runner = active_runner.kill_wb();
    // TODO verify correct JSON data was written
    let active_runner = runner.start_wb(false);
    let (mut client, tests) = active_runner.start_client_test();
    active_runner.run_test(&mut client, &tests[1]);
    active_runner.shutdown_wb();
    // TODO verify grave goods and last will have been cleaned up
}

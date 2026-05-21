mod test_common;

use crate::test_common::TestRunner;

#[cfg(feature = "sqlite")]
#[test]
fn grave_goods_and_last_will_are_presisted_with_sqlite_storage_and_applied_after_crash() {
    let runner = TestRunner::new(
        "./tests/json/server_setup_sqlite.json",
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
    std::thread::sleep(std::time::Duration::from_millis(100));
    let runner = active_runner.kill_wb();
    // TODO verify correct JSON data was written
    let active_runner = runner.start_wb(false);
    let (mut client, tests) = active_runner.start_client_test();
    active_runner.run_test(&mut client, &tests[1]);
    active_runner.shutdown_wb();
    // TODO verify grave goods and last will have been cleaned up
}

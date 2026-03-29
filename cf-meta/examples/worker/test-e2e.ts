/**
 * End-to-end test for the wskv worker + container.
 *
 * This test verifies the full JuiceFS Cloudflare integration:
 *   - Container boots with the sandbox agent
 *   - Go cfmount binary starts and connects via WebSocket
 *   - DO wskv server handles metadata over SQLite
 *   - FUSE mount works at /mnt/jfs
 *   - File I/O round-trips through JuiceFS (metadata via wskv, data via R2)
 *
 * Usage:
 *   pnpm test:e2e                          # run against deployed worker
 *   WSKV_URL=http://localhost:8787 pnpm test:e2e  # run against local wrangler dev
 */

const BASE = process.env.WSKV_URL ?? "https://wskv-example.ramon3525.workers.dev";
const NAME = "test";

async function main() {
  console.log(`Testing against ${BASE}\n`);

  // 1. Boot the container (first exec triggers container start + onStart)
  console.log("=== Booting container ===");
  const whoami = await exec("whoami");
  assert(whoami.success, "whoami should succeed");
  assert(whoami.stdout === "root", `Expected root, got: ${whoami.stdout}`);
  console.log("Container is running as root.\n");

  // 2. Establish the wskv WebSocket connection (triggers volume format + FUSE mount)
  console.log("\n=== Connecting wskv ===");
  const connectResp = await fetch(`${BASE}/connect/${NAME}`, { method: "POST" });
  if (!connectResp.ok) {
    const body = await connectResp.text();
    throw new Error(`connect failed: ${connectResp.status} ${body.slice(0, 500)}`);
  }
  console.log("wskv connected:", await connectResp.json());

  // 4. Wait a moment for FUSE mount to complete after init handshake
  console.log("\n=== Checking FUSE mount ===");
  const mount = await waitForMount();
  console.log("mount info:", mount);

  assert(mount.includes("fuse"), "Expected FUSE filesystem at /mnt/jfs");
  console.log("FUSE mount is active.\n");

  // 5. Test file operations through JuiceFS
  console.log("=== Testing JuiceFS file I/O ===");
  const testId = Date.now().toString(36);

  console.log("--- write file ---");
  const write = await exec(`echo "juicefs-${testId}" > /mnt/jfs/test-${testId}.txt`);
  assert(write.success, `write failed: ${write.stderr}`);

  console.log("--- read file ---");
  const read = await exec(`cat /mnt/jfs/test-${testId}.txt`);
  assert(read.stdout === `juicefs-${testId}`, `Expected juicefs-${testId}, got: ${read.stdout}`);
  console.log(`Read back: "${read.stdout}"`);

  console.log("--- create nested directory ---");
  const mkdir = await exec(`mkdir -p /mnt/jfs/sub-${testId}/deep`);
  assert(mkdir.success, `mkdir failed: ${mkdir.stderr}`);

  console.log("--- write nested file ---");
  const write2 = await exec(`echo "nested-${testId}" > /mnt/jfs/sub-${testId}/deep/file.txt`);
  assert(write2.success, `nested write failed: ${write2.stderr}`);

  console.log("--- stat file (verify inode metadata) ---");
  const stat = await exec(`stat /mnt/jfs/test-${testId}.txt`);
  assert(stat.success, `stat failed: ${stat.stderr}`);
  console.log(stat.stdout);

  console.log("--- list all files ---");
  const find = await exec(`find /mnt/jfs -path "*${testId}*" -type f`);
  assert(find.stdout.includes(`test-${testId}.txt`), "Expected test file in listing");
  assert(find.stdout.includes(`sub-${testId}/deep/file.txt`), "Expected nested file in listing");
  console.log(find.stdout);

  console.log("--- delete files ---");
  const rm = await exec(`rm -r /mnt/jfs/test-${testId}.txt /mnt/jfs/sub-${testId}`);
  assert(rm.success, `rm failed: ${rm.stderr}`);

  console.log("--- verify deleted ---");
  const verify = await exec(`ls /mnt/jfs/test-${testId}.txt 2>&1; echo "EXIT:$?"`);
  assert(verify.stdout.includes("No such file") || verify.stdout.includes("cannot access"),
    "File should be deleted");

  console.log("\n=== All tests passed! ===");
  console.log("JuiceFS is running on Cloudflare: metadata via wskv (DO SQLite), data via R2.");
}

interface ExecResult {
  stdout: string;
  stderr: string;
  exitCode: number;
  success: boolean;
}

async function exec(cmd: string): Promise<ExecResult> {
  const resp = await fetch(`${BASE}/exec/${NAME}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ cmd }),
  });
  if (!resp.ok) {
    throw new Error(`exec failed: ${resp.status} ${await resp.text()}`);
  }
  return resp.json() as Promise<ExecResult>;
}

async function waitForMount(): Promise<string> {
  for (let i = 0; i < 30; i++) {
    const result = await exec("mount | grep /mnt/jfs || echo 'NOT MOUNTED'");
    if (!result.stdout.includes("NOT MOUNTED")) {
      return result.stdout;
    }
    console.log(`  waiting for FUSE mount... (${i + 1}/30)`);
    await new Promise((r) => setTimeout(r, 2000));
  }

  // Diagnostics on failure
  const logs = await exec("cat /tmp/cfmount.log 2>/dev/null || echo 'no log file'");
  console.log("cfmount logs:", logs.stdout);
  const fuse = await exec("ls -la /dev/fuse 2>&1 || echo 'NO /dev/fuse'");
  console.log("/dev/fuse:", fuse.stdout);

  throw new Error(
    "FUSE mount not active at /mnt/jfs after 60s. " +
    "The WebSocket connection or FUSE setup may have failed."
  );
}

function assert(cond: boolean, msg: string) {
  if (!cond) throw new Error(`Assertion failed: ${msg}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

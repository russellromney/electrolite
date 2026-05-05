import { copyFileSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const here = dirname(fileURLToPath(import.meta.url));
const pkg = join(here, "..");
const repo = join(pkg, "..", "..");

const cargo = spawnSync("cargo", ["build", "-p", "electrolite-node-native"], {
  cwd: repo,
  stdio: "inherit",
});
if (cargo.status !== 0) {
  process.exit(cargo.status ?? 1);
}

const platform = process.platform;
const arch = process.arch;
const sourceName =
  platform === "win32"
    ? "electrolite_node_native.dll"
    : platform === "darwin"
      ? "libelectrolite_node_native.dylib"
      : "libelectrolite_node_native.so";

const nativeDir = join(pkg, "native");
mkdirSync(nativeDir, { recursive: true });
copyFileSync(
  join(repo, "target", "debug", sourceName),
  join(nativeDir, `electrolite_node_native.${platform}-${arch}.node`),
);


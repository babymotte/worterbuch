#!/usr/bin/env node

const fs = require("fs");
const path = require("path");

const root = path.resolve(process.argv[2] || ".");
const ignoredDirs = new Set(["target", "node_modules", ".git"]);
const headerPattern = /^ \*  Copyright \(C\)/m;

function* findRsFiles(dir) {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      if (!ignoredDirs.has(entry.name)) {
        yield* findRsFiles(fullPath);
      }
    } else if (entry.isFile() && entry.name.endsWith(".rs")) {
      yield fullPath;
    }
  }
}

const missing = [];
for (const file of findRsFiles(root)) {
  if (!headerPattern.test(fs.readFileSync(file, "utf8"))) {
    missing.push(path.relative(root, file));
  }
}

missing.sort().forEach((file) => console.log(file));
process.exitCode = missing.length > 0 ? 1 : 0;

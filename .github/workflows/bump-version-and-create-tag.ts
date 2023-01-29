#!/usr/bin/env -S deno run -A

import {assert, assertEquals} from "https://deno.land/std@0.170.0/testing/asserts.ts";
import * as semver from "https://deno.land/x/semver@v1.4.1/mod.ts";


function bumpVersionInCargoToml(releaseType: semver.ReleaseType): string {
    const cargoToml = Deno.readTextFileSync("Cargo.toml");
    const [fullLine, oldVersion] = cargoToml.match(/version = "([^"]+)"/);
    const newVersion = semver.inc(oldVersion, releaseType);
    assert(newVersion);
    Deno.writeTextFileSync("Cargo.toml", cargoToml.replace(fullLine, `version = "${newVersion}"`));
    return newVersion;
}

async function updateVersionInCargoLock() {
    await exec(["cargo", "check"]);
}

async function createTag(tag: string) {
    await exec(["git", "add", "Cargo.toml", "Cargo.lock"]);
    await exec(["git", "commit", "-m", `Release ${tag}`]);
    await exec(["git", "tag", tag]);
    await exec(["git", "push"]);
    await exec(["git", "push", "origin", tag]);
}

function appendToOutputFile(path: string, version: string, tag: string) {
    Deno.writeTextFileSync(path, `VERSION=${version}\n`, {append: true});
    Deno.writeTextFileSync(path, `TAG=${tag}\n`, {append: true});
}

async function exec(cmd: string[]) {
    console.log(cmd.join(" "));
    const status = await Deno.run({cmd}).status();
    assert(status.success);
}


assertEquals(Deno.args.length, 2);
const releaseType = Deno.args[0] as semver.ReleaseType;
const outputFilePath = Deno.args[1];
console.log('releaseType:   ', releaseType);
const newVersion = bumpVersionInCargoToml(releaseType);
const tag = `v${newVersion}`;
console.log('newVersion:    ', newVersion);
console.log('tag:           ', tag);
appendToOutputFile(outputFilePath, newVersion, tag);
await updateVersionInCargoLock();
await createTag(tag);

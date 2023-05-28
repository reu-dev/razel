#!/usr/bin/env -S deno run -A

import {assert, assertEquals} from "https://deno.land/std@0.170.0/testing/asserts.ts";
import * as semver from "https://deno.land/x/semver@v1.4.1/mod.ts";


function bumpVersionInCargoToml(releaseType: semver.ReleaseType): [string, string] {
    const cargoToml = Deno.readTextFileSync("Cargo.toml");
    const matchResults = cargoToml.match(/version = "([^"]+)"/);
    assert(matchResults);
    const [fullLine, oldVersion] = matchResults;
    const newVersion = semver.inc(oldVersion, releaseType);
    assert(newVersion);
    Deno.writeTextFileSync("Cargo.toml", cargoToml.replace(fullLine, `version = "${newVersion}"`));
    return [oldVersion, newVersion];
}

async function updateVersionInCargoLock() {
    await exec(["cargo", "update", "-p", "razel"]);
}

async function updateVersionInApis(oldVersion: string, newVersion: string) {
    assert(!oldVersion.startsWith('v'));
    assert(!newVersion.startsWith('v'));
    for (const [file, matcher] of [
        ["include/deno/razel.ts", `version = "${oldVersion}"`],
        ["include/python/razel.py", `version: ClassVar.str. = "${oldVersion}"`],
        ["test/deno.ts", `razel@v${oldVersion}`],
        ["deno.json", `razel@v${oldVersion}`],
    ]) {
        const content = Deno.readTextFileSync(file);
        const matchResults = content.match(matcher);
        assert(matchResults);
        const [oldLine] = matchResults;
        const newLine = oldLine.replace(oldVersion, newVersion);
        Deno.writeTextFileSync(file, content.replace(oldLine, newLine));
        await exec(["git", "add", file]);
    }
}

async function createTag(tag: string) {
    await exec(["git", "diff", "--cached"]);
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
const [oldVersion, newVersion] = bumpVersionInCargoToml(releaseType);
const tag = `v${newVersion}`;
console.log('oldVersion:    ', oldVersion);
console.log('newVersion:    ', newVersion);
console.log('tag:           ', tag);
appendToOutputFile(outputFilePath, newVersion, tag);
await updateVersionInCargoLock();
await exec(["git", "add", "Cargo.toml", "Cargo.lock"]);
await updateVersionInApis(oldVersion, newVersion);
await createTag(tag);

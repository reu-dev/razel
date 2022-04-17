# razel tests

* Execute commands from a [batch file](batch.sh)
    ```
    razel batch batch.sh
    ```

* Execute commands from a [razel.jsonl file](razel.jsonl) created by a [Deno](https://deno.land/) [script](deno.ts)
    ```
    deno run --allow-write=. deno.ts
    razel build
    ```

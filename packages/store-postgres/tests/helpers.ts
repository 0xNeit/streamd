import { afterEach } from "bun:test";
import { PGlite as BasePGlite } from "@electric-sql/pglite";

const opened: BasePGlite[] = [];

afterEach(async () => {
  await Promise.all(opened.splice(0).map((db) => db.close().catch(() => {})));
});

/** Test `PGlite` instance closed after each test so RSS stays bounded across files. */
export class PGlite extends BasePGlite {
  constructor() {
    super();
    opened.push(this);
  }
}

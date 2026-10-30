# Contributing

## Development setup

Phoenix Core requires JDK 21. Gradle is provided by the wrapper.

```bash
./gradlew clean build    # compile, test, Javadoc, package
./gradlew test --tests 'com.phoenix.net.TaskQueueServiceTest'
```

Test reports are written to `build/reports/tests/test/index.html`.

The MQTT broker integration tests (`MqttMosquittoIntegrationTest`) start a real
Mosquitto with the files in `deploy/mosquitto`. They need `mosquitto`,
`mosquitto_passwd`, `openssl` and `bash`, and are skipped when any is missing:

```bash
sudo apt install mosquitto      # Debian/Ubuntu; CI installs it too
brew install mosquitto          # macOS
```

The build treats compiler warnings as errors (`-Xlint:all -Werror`) and
`check` builds the Javadoc with doclint, so a broken `{@link}` or malformed
HTML in a doc comment fails the build.

## Continuous integration

Every pull request and every push to `main` runs:

- **Build and test**: `./gradlew clean build` on JDK 21.
- **Repo hygiene**: rejects tracked build output and whitespace errors.

Both checks must pass before a pull request is merged.

## Conventions

- Branches and commit titles use a type prefix: `feat`, `fix`, `refactor`, `docs`, `test`, `ci`, `chore`.
- Keep pull requests to a single change, with tests for new behaviour and bug fixes.
- Tests must be deterministic: use the injectable `Clock` and id-supplier seams rather than
  `Thread.sleep`, and bind servers to port `0`.
- Javadoc describes behaviour and contracts. Issue history belongs in commit messages and
  pull requests, not in doc comments.

## Workflow

Every issue, in every Phoenix repository, goes through the same seven steps, one
bounded change at a time.

1. **Inspect.** Move the issue to In progress and assign yourself. Open every
   file in its Inspect first list, and its tests.
2. **Validate.** Compare the issue's Why and Scope with what the code does today,
   and record the result in its Verified against section. If the code disagrees
   with the issue, fix the issue before writing code.
3. **Scope.** Confirm Out of scope still holds. Extra work found along the way
   becomes a new issue rather than growing this one.
4. **Implement.** Branch from an up-to-date `main` and make the smallest change
   that satisfies Done when.
5. **Test.** Run the full local check (`./gradlew clean build` in this
   repository) before pushing.
6. **Review.** Open a pull request with the template. Its body says
   `Closes <repo>#<issue>`, or `Part of ...` when the issue needs several PRs
   (only the last one says `Closes`). CI must be green.
7. **Merge.** Squash-merge with the PR title as the commit title, with the change
   listed under Unreleased in `CHANGELOG.md` in the same PR.

Branches are named `<type>/<issue#>-<slug>`, for example
`feat/31-task-input`. `<type>` matches the pull request title prefix: `feat`,
`fix`, `refactor`, `docs`, `test`, `ci`, `chore` or `tooling`.

### Changes that cross repositories

When a change affects what Core and the agents say to each other, it happens in
this order:

1. **Spec PR in Core.** Update `docs/mqtt-agent-protocol.md` (and the HTTP docs
   where relevant) with the new fields, the protocol feature that announces them
   and the compatibility rule. Docs only.
2. **Core implementation PR(s).** Core supports the new behaviour only for agents
   that announce the feature, so older agents keep working.
3. **Agent PRs.** Linux and Android in parallel, each closing its own issue that
   says `Depends on` the Core issue.
4. **Integration run on real devices**, recorded in the experiment record.
5. **Release** at the milestone end: Core first, then the agents, then the
   compatibility table in Core's README.

Cross-repository references are written in full, for example
`Depends on Phoenix-Mesh/phoenix#31`.

## Releasing

1. Set `version` in `build.gradle.kts` and add a matching section to `CHANGELOG.md`.
2. Merge to `main` with CI green.
3. Tag the merge commit and push the tag:

   ```bash
   git tag -a v0.2.1 -m "Phoenix Core v0.2.1"
   git push origin v0.2.1
   ```

The `Release` workflow verifies that the tag matches the project version, builds,
and publishes a GitHub release with the distribution archives and the changelog section.

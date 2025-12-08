# Development guide
### Launch the project

Requirements:
* Java 21 (we recommend using [sdkman!](https://sdkman.io/) to install and manage versions of the JDK)

To develop on a specific service, you can use the provided `docker-compose.yml` to launch the dependencies:

```shell script
docker compose -f docker-compose-dependencies.yml up -d
```

Then, from the root directory, launch the service:

```shell script
./gradlew search-service:bootRun
```

### Running the tests

Each service has a suite of unit and integration tests. You can run them without manually launching any external component, thanks
to Spring Boot embedded test support and to the great [TestContainers](https://www.testcontainers.org/) library.

For instance, you can launch the test suite for entity service with the following command:
 
```shell script
./gradlew search-service:test
```

### Building the project

To build all the services, you can launch:

```shell script
./gradlew build
```

It will compile the source code, check the code quality (thanks to [detekt](https://detekt.dev/))
and run the tests for all the services.

For each service, a self-executable jar is produced in the `build/libs` directory of the service.

If you want to build only one of the services, you can launch:

```shell script
./gradlew search-service:build
```

### Committing

* Commits follow the [Conventional Commits specification](https://www.conventionalcommits.org/en/v1.0.0/)
* Branches follow the [Conventional Branch specification](https://conventional-branch.github.io/)
  * In the context of the project, the following prefixes are authorized when naming branches: `feature/`, `refactor/`,
    `fix/`, `hotfix/` and `chore/`.


### Code quality

Code formatting and standard code quality checks are performed by [Detekt](https://detekt.github.io/detekt/index.html).
Detekt as an autocorrect command to help on easily fixable formatting:
```shell script
./gradlew detekt --auto-correct
``` 

Detekt checks are automatically performed as part of the build and fail the build if any error is encountered.

* You may consider using a plugin like [Save Actions](https://plugins.jetbrains.com/plugin/7642-save-actions) 
that applies changed code refactoring and optimized imports on save.

* You can enable Detekt support with the [Detekt plugin](https://github.com/detekt/detekt-intellij-plugin).

* You can also set up a precommit hook to run detekt autocorrect automatically 

### Pre-commit

#### Automatic setup with [pre-commit](https://pre-commit.com/) tool 

(if you don't have Python installed, use the manual setup below)
* install ```pip install pre-commit```
* then run ```pre-commit install```
 
#### Manual setup

* copy the script in ```config/detekt/detekt_auto_correct.sh``` in your ```.git/pre-commit``` file

## Releasing a new version

* Merge `develop` into `master` 

```
git checkout master
git merge develop
```

* Update version number in `build.gradle.kts` (`allprojects.version` near the bottom of the file) and in `.env` (`STELLIO_DOCKER_TAG` environment variable)
* Commit the modification using the following template message

```
git commit -am "chore: upgrade version to x.y.z"
```

* Push the modifications

```
git push origin master
```

The CI will then create and publish Docker images tagged with the published version number in https://hub.docker.com/u/stellio.

* On GitHub, check and publish the release notes in https://github.com/stellio-hub/stellio-context-broker/releases

## Contribution 

guideline for contributing are [available here](https://github.com/stellio-hub/stellio-context-broker/blob/develop/CONTRIBUTING.md)
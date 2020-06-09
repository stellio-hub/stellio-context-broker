# Contributing

Stellio is an open source project part of FIWARE. We welcome any contributions from everyone.  
If you wish to contribute on our code, you should first read and follow guidelines mentionned in this document.

## Ask a question 

Please if you want to ask a question regarding Stellio and how to use it. Create an issue
 in [Stackoverflow](https://stackoverflow.com/) with the tag fiware-stellio.

## Reporting Issues

You may find an issue when using Stellio, if so, please check that an issue is not already reported. If there are not, 
please create it with as much information as possible :

- Version of the project linked to the issue.
- Explain expected and actual behaviour.
- Steps to reproduce the issue.

There are many labels available, please feel free to put a label on your issue.

## Requesting new features

Stellio is implementing the NGSI-LD API specification.  You may find unsupported features.  
When requesting a new feature, please provide the section and subsections related to the feature in the NGSI-LD 
specification. Please provide if possible the version of the specification.

Good example : 
https://github.com/stellio-hub/stellio-context-broker/issues/12

## Making Changes

Contributions to Stellio are done using a pull request. Find below a description on how to do a good pull request :

- Fork this repository
- Create a branch on the forked repository.  
A good branch name should follow this schema : `<label>/<issue_number>-<name>`

    - `<label>` is the label of your issue : feature, bug, refactor, documentation...
    - `<issue_number>` is the number of the issue
    - `<name>` is the name of the feature, with `-` instead of spaces.

For example : `documentation/60-add-contributing-file`

- Commit your work and push to the branch.  
A good commit message should follow this schema : `<label>: <commit msg> #<issue_number>`

    - `<label>` is the label of your issue : feature, bug, refactor, documentation...
    - `<commit_msg>` is the short description of your commit
    - `<issue_number>` is the number of the issue

For example : `documentation: add contributing.md file #60`

- Submit a pull request to the target branch in the main repository
- Wait for reviews from [Stellio maintainers](https://github.com/orgs/stellio-hub/people)
- Stay reactive to comments

When a user is starting a comment thread, he should resolve it once he thinks the discussion has ended.

Once there is agreement that the code is ready to be included in the main project, one of Stellio's
maintainers will merge your contribution.

Increase the chances that your pull request will be accepted and gain time :

- Follow our [coding style](https://github.com/stellio-hub/stellio-context-broker/blob/develop/docs/CONTRIBUTING.md#coding-style).
- Write tests for your changes.
- Write a good commit title, add description if there are many changes.

If your pull request is too big, you might want to split it into smaller pull requests.

Small pull requests lead to fast reviews and a better understanding of the changes.

A good guide on why you should split a pull request and how : https://www.thedroidsonroids.com/blog/splitting-pull-request

## Project Structure

Stellio is divided into five main modules: api-gateway, entity-service, search-service, subscription-service and shared.
Each module follows the same schema :
- **src**:
    - **main**: contains the source code.
        - **kotlin**: contains the source kotlin code.
        - **resources** : contains the resources.
    - **test**: contains the test code, a unit test class of a source code class should have the same package.

The project contains also directories linked to documentation and build :

- **docs**: contains documentation linked to the project.
- **gradle**: contains tools to build the project.

## Coding style

We follow [ktlint](https://ktlint.github.io/#rules) coding style rules.

To format the code, we use Intellij built-in formatter with settings that can be found in the `config/settings` directory.

Import settings with `File/Manage IDE Settings/Import Settings...`.

You can use plugins like [Save Actions](https://plugins.jetbrains.com/plugin/7642-save-actions) that applies 
changed code formatting and optimized imports on a save.

## License

By contributing to this project, you agree that your contributions will be
licensed under its [license](LICENSE).

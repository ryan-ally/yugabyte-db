---
title: Build the YugabyteDB docs locally
headerTitle: Build the docs
linkTitle: Build the docs
description: Build the YugabyteDB docs locally
image: /images/section_icons/index/quick_start.png
type: page
menu:
  preview:
    identifier: docs-build
    parent: docs
    weight: 2913
isTocNested: true
showAsideToc: true
---

## Prerequisites

To run the docs site locally and edit the docs, you'll need:

* **A text editor**, such as [Visual Studio Code](https://code.visualstudio.com)

* **Command-line tools for Xcode** on macOS.

    ```sh
    $ xcode-select --install
    ```

    \
    Xcode is many gigabytes. Install the command-line tools unless you actually need the full Xcode.

* [**Homebrew**](https://brew.sh) on macOS or Linux.

* **Node.js** v16.x, installable in several ways:

  * From the [node.js website](https://nodejs.org/en/download/)
  * Using Homebrew: `brew install node@16`
  * Using NVM: `nvm use 16`

* **Hugo**: `brew install hugo` gets you the latest version.

* **A GitHub account**

* **Git client**: The system Git binary is out of date, but works. If you like, you can use Homebrew to get a newer version (`brew install git`).

## Fork the repository

1. To make the commands in this section work correctly when you paste them, set an environment variable to store your GitHub username.

    ```sh
    export GITHUB_ID=your-github-id
    ```

1. Fork the `yugabyte-db` GitHub repository and create a local clone of your fork with a command like this:

    ```sh
    git clone https://github.com/$GITHUB_ID/yugabyte-db.git
    ```

1. Identify your fork as _origin_ and the original YB repository as _upstream_:

    ```sh
    cd yugabyte-db/
    git remote set-url origin https://github.com/$GITHUB_ID/yugabyte-db.git
    git remote add upstream https://github.com/yugabyte/yugabyte-db.git
    ```

1. Make sure that your local repository is still current with the upstream Yugabyte repository:

    ```sh
    git checkout master
    git pull upstream master
    git push origin
    ```

## Build the docs site {#live-reload}

The YugabyteDB documentation is written in Markdown, and processed by Hugo (a static site generator) into an HTML site.

To get the docs site running in a live-reload server on your local machine, run the following commands:

```sh
cd yugabyte-db/docs  # Make sure this is YOUR fork.
npm ci               # Only necessary the first time you clone the repo.
npm start            # Builds the docs and launches the live-reload server.
```

The live-reload server runs at <http://localhost:1313/> unless port 1313 is already in use. Check the output from the `npm start` command to verify the port in use.

When you're done, type Ctrl-C stop the server.

{{< note title="Not looking quite right?" >}}
There's a transient bug in the live-reload build. If your local docs site doesn't look right, type Ctrl-C and re-run the `npm start` command.
{{< /note >}}

### Optional: Run a full build {#full-build}

The live-reload server is the quickest way to get the docs running locally. If you want to run the build exactly the same way the CI pipeline does for a deployment, do the following:

```sh
cd yugabyte-db/docs
npm run build
```

When the build is done, the `yugabyte-db/docs/public` folder contains a full HTML site, exactly the same as what's deployed on the live website at <https://docs.yugabyte.com/>.

## Troubleshooting

* Make sure the GUI installer for the command-line tools finishes with a dialog box telling you the install succeeded. If not, run it again.

* If you get an error about missing command-line tools, make sure xcode-select is pointing to the right directory, and that the directory contains a `usr/bin` subdirectory. Run `xcode-select -p` to find the path to the tools. Re-run xcode-select --install.

* If the live-reload site looks odd, stop the server with Ctrl-C and re-run `npm start`.

## Next steps

Need to edit an existing page? [Start editing](../docs-edit/) it now. (Optional: [set up your editor](../docs-editor-setup/).)

Adding a new page? Use the [overview of sections](../docs-layout/) to find the appropriate location.

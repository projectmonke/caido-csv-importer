# Caido Importer
A Go utility that allows you to import HTTP requests into Caido projects from CSVs.

# Installation
- Clone this repo to your local machine.
- Run `go build` to get your binary.

# Usage
- Create a new Caido project. In the `Workspace` menu, click the three dots next to the project to copy the project path.
- The CSV to import should be in the format of exported Caido requests. That is, when you export HTTP requests via Logger or HTTP History, this utility allows you to re-import these requests to a new project.
- Use the `-f` flag to specify the CSV location, and the `-p` flag to specify the project path.

# Disclaimer
This tool was created using [Burp2Caido](https://github.com/caido-community/burp2caido)'s logic as a template, and Gemini oneshotted the rest. Credit for the main logic goes to the Caido team. As usual, this tool should be used for ethical purposes only and I am not responsible for any misuse of this tool. This is developed under the GNU General Public License v3.0, so you are free to modify, distribute and use this tool however you wish.

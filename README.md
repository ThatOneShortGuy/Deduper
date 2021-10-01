# Deduper
### **Be sure to run command** `pip install progressbar_easy` **or module will not work!**
## Goal
The goal of this project is to make a deduper program that anybody can run on their computer to save storage space.
## Description
* [R_deduper1.1](https://github.com/ThatOneShortGuy/Deduper/blob/main/R_deduper1.1.py) is used to dedupe all files in the current directory and subdirectories of the file together.
* [R_undeduper_diving](https://github.com/ThatOneShortGuy/Deduper/blob/main/R_undeduper_diving.py) is used for undoing everything that [R_deduper1.1](https://github.com/ThatOneShortGuy/Deduper/blob/main/R_deduper1.1.py) does. It will undedupe all deduped files in current and sub directories.
* [R_undeduper](https://github.com/ThatOneShortGuy/Deduper/blob/main/R_undeduper.py) is meant to be packaged as an .exe file with pyinstaller to allow clicking on the deduped file to have that individual file undeduped and ready for use. Use command `pyinstaller R_undeduper.py` to package as .exe. Move the .exe and its dependencies to your programs folder. Click to open a .deduped file and select "open with." Then navigate to .exe file and select it.
## What to expect
When running [R_deduper1.1](https://github.com/ThatOneShortGuy/Deduper/blob/main/R_deduper1.1.py), it will read through all the data and predict how much data should be saved. A new file will then be created title "DeTable.pickle" and it will be filled with all the data that is going to be pulled out. As the files are being written to, they will appear as a new file. When they are finished being written to, the file metadata from the previous file will be coppied to the .deduped file and the previous one will be erased.

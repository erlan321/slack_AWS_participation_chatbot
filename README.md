# slack_AWS_participation_chatbot
Instructions for building a chatbot in AWS to grade student participation in a Slack channel

This repository is a modified version of an original tool created by Dr. Elle O'Brien of the University of Michigan, whose repository can be found here: https://github.com/elleobrien/participation-bot.  Many thanks to Elle for her help and advice on this project!  

This modified version of Elle's original tool allows a single Slack Chat Bot, hosted by Amazon Web Services (AWS) using their Lamba Function feature, to provide participation grades for any Slack channel for different types of courses in the University of Michigan Masters in Applied Data Science (MADS) program.  
 - In a project-oriented course, teams of students (or individual students) are expected to post periodic "stand ups" about their project on Slack, and then also review and comment/advise their fellow students' stand ups.
 -  In a course that requires weekly reading assignments (or a similar weekly material review) where individual students are expected to post their own thoughts on the reading on Slack, and then also review and discuss the thoughts of their fellow students.
 - In a course that might have a more limited discussion requirement, an instructor might created a Pinned Thread in the course's main Slack channel with a weekly discussion topic for which each student is expected to share their thoughts.
 
The documentation for creating the Bot credentials, creating the AWS Lambda Function, and using the Bot with Slack Channels can be found here (requires you are logged in to your Umich email address): 
  https://docs.google.com/document/d/11PJOHlkSetp_17w3eq4JKHlyw0Bi-byxRICf2rIkaSc/edit?usp=sharing 
  
 This Github repository contains the two files needed for the AWS Slack Chatbot to work:
  - The primary lambda_function.py code that runs the bot.
  - A sample credentials.json file that would contain your own Bot's credentials in the same format as the one shown.
  


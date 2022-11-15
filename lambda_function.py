import json
import requests
import pandas as pd
from time import sleep
import datetime
import collections



########## START - functions that send data back to Slack channels ##########     

#Generic error message with some details the user can check on their own.    
def return_instruction_message(channel_id, BOT_TOKEN):
    url = "https://slack.com/api/chat.postMessage" #docs https://api.slack.com/methods/chat.postMessage
    headers = {"Authorization": "Bearer "+BOT_TOKEN, 'content-type': "application/json"}
    params = {}
    params["channel"] = channel_id
    params["attachments"] = [{"pretext": "Hi!  Looks like you want some grading done.  Please check the following items and send to me again...", "text": "(1a) If you are grading a channel, I need the parameters in this format: \n <bot_name> <channel-name> <from-date> <to-date> \n @bot siads999_ss22_course_name 08/22/2022 08/28/2022 \n \n (1b) Or, if you are set up for grading Pinned Threads, use this format: \n  <bot_name> <channel-name> <from-date> <to-date> <thread-name> \n @bot siads999_ss22_course_name 08/22/2022 08/28/2022 Week1_Discussion \n \n (2) If the above are correct, you may need to check the setup of your course in the Google Sheet, where you set the minimum required posts & replies and the grade point value.  Team graded classes need to be designated as such and require a separate sheet listing the team assignments.  Pinned Thread Graded channels need to be designated as such and can only grade posts to the thread (replies must be set to zero). \n \n (3)The bot needs to be a member of both the instructor channel and the student course channel you are grading. \n \n (4) If all the above check out, please contact the appropriate support person."},]
    r = requests.post(url, headers=headers, json=params)
    return (r, r.url)

#A simple sample message that can be a template for dev/debugging.    
# def return_simple_message(channel_id, BOT_TOKEN):
#     url = "https://slack.com/api/chat.postMessage" #docs https://api.slack.com/methods/chat.postMessage
#     headers = {"Authorization": "Bearer "+BOT_TOKEN, 'content-type': "application/x-www-form-urlencoded"}
#     params = {}
#     params["channel"] = channel_id
#     params["text"] = "Thanks, your parameters are correct!"
#     r = requests.post(url, headers=headers, params=params)
#     return (r, r.url)

#This function returns raw data channel data to the instructor, NOT finished grading.  For use in dev/debugging.    
def return_raw_data(df, channel_id, BOT_TOKEN):
    url = "https://slack.com/api/files.upload" #docs https://api.slack.com/methods/files.upload
    headers = {"Authorization": "Bearer "+BOT_TOKEN, 'content-type': "application/x-www-form-urlencoded"}
    #headers = {"Authorization": "Bearer "+BOT_TOKEN, 'content-type': "multipart/form-data"}
    params = {}
    params["channels"] = channel_id #note the use of "channels" rather than "channel"
    params["initial_comment"] = "Here is a raw data file of class participation."
    #params["content"] = open('raw.csv', 'r').read() #this workds!
    params["content"] = str(df.to_csv()) #this works!
    params["filename"] = "raw_activity.csv"
    params["filetype"] = "csv"
    r = requests.post(url, headers=headers, params=params)
    return (r, r.url)    

#A function that returns the final graded data to the instructor.  This is the end product of the Bot.     
def return_grade_data(grade_filename, df, channel_id, BOT_TOKEN):
    url = "https://slack.com/api/files.upload" #docs https://api.slack.com/methods/files.upload
    headers = {"Authorization": "Bearer "+BOT_TOKEN, 'content-type': "application/x-www-form-urlencoded"}
    params = {}
    params["channels"] = channel_id #note the use of "channels" rather than "channel"
    params["initial_comment"] = "Here is a graded data file of class participation."
    params["content"] = str(df.to_csv()) 
    params["filename"] = grade_filename
    params["filetype"] = "csv"
    r = requests.post(url, headers=headers, params=params)
    return (r, r.url)  

########## END - functions that send data back to Slack channels ##########     
########## START - functions that retrieve data from the Student Slack Channel ##########     

#find the id code of the Slack Channel name
def get_channel_id(channel_name, BOT_TOKEN): 
    url = "https://slack.com/api/conversations.list" #docs: https://api.slack.com/methods/conversations.list
    headers = {"Authorization": "Bearer "+BOT_TOKEN, "content-type": "application/x-www-form-urlencoded"}
    params = {}
    params["types"] = "private_channel" #we opted not to include the option for"public_channel", this creates too many channels to search through
    params["exclude_archived"] = True
    params["limit"] = 200

    max_pages = 5
    keep_looking = True
    page = 1
    channel_id = None #initiate at None
    count = 0
    wait = 1

    while keep_looking == True and page <= max_pages:
        if page == 1:
            results = requests.get(url,headers=headers,params=params)
            print(results.url)
            results = results.json()
            for channel in results["channels"]:
                count +=1
                print(channel["name"], count)
                if channel["name"] == channel_name:
                    
                    channel_id = channel["id"]
                    keep_looking = False
                    return channel_id
            cursor = results["response_metadata"]["next_cursor"] #if the channel_id has not been found, move to next page
            page +=1
            #sleep(wait)
        else:
            params["cursor"] = cursor #update the params for the next page
            results = requests.get(url,headers=headers,params=params)
            print(results.url)
            results = results.json()
            for channel in results["channels"]:
                count +=1
                print(channel["name"], count)
                if channel["name"] == channel_name:
                    channel_id = channel["id"]
                    keep_looking = False
                    return channel_id
            cursor = results["response_metadata"]["next_cursor"] #if the channel_id has not been found, move to next page
            page +=1
            #sleep(wait)

    return channel_id
    
#main function that generates raw message data from the channel.  Note that this will NOT return the full text of replies to posts.
def get_all_posts_in_channel(channel_id, BOT_TOKEN, from_date, to_date, max_pages = 5): 
    url = "https://slack.com/api/conversations.history" #docs https://api.slack.com/methods/conversations.history
    headers = {"Authorization": "Bearer "+BOT_TOKEN, "content-type": "application/x-www-form-urlencoded"}
    params = {}
    params["channel"] = channel_id
    params["oldest"] = datetime.datetime.strptime(from_date, "%m/%d/%Y").timestamp()
    params["latest"] = datetime.datetime.strptime(to_date, "%m/%d/%Y").timestamp()
    params["limit"] = 200

    max_pages = max_pages
    keep_looking = True
    page = 1
    channel_id = None #initiate at None
    count = 0
    wait = 1

    all_messages = [] #initiate blank list to hold messages
    while keep_looking == True and page <= max_pages:
        if page == 1:
            result = requests.get(url,headers=headers,params=params)
            #print(result.url)
            result = result.json()
            sleep(wait)
        else:
            params["cursor"] = result['response_metadata']['next_cursor'] #set the next page cursor
            result = requests.get(url,headers=headers,params=params)
            #print(result.url)
            result = result.json()
        all_messages = all_messages + result['messages']
        keep_looking = result['has_more']
        sleep(wait)
        page +=1    
    # for msg in all_messages:
    #     print(msg["text"])
    return(all_messages)

#used to get all users in a channel (for non-team grading)
def get_all_users_in_channel(channel_id, BOT_TOKEN):
    url = "https://slack.com/api/conversations.members" #docs https://api.slack.com/methods/conversations.members
    headers = {"Authorization": "Bearer "+BOT_TOKEN, 'content-type': "application/x-www-form-urlencoded"}
    params = {}
    params["channel"] = channel_id
    params["limit"] = 200

    users = requests.get(url, headers=headers, params=params)
    #print(users.url)
    users = users.json()
    users = users['members']
    #print(users)
    uniqnames = user_id_to_uniqname(users, BOT_TOKEN)
    all_users_df = pd.DataFrame(zip(users,uniqnames), columns=['user_id','uniq_name'])
    return(all_users_df)

#matches slack user id to uniqname.  The uniqname is the common identifier of a student between Slack and Coursera
def user_id_to_uniqname(user_id_list, BOT_TOKEN):
    url = "https://slack.com/api/users.info" #docs https://api.slack.com/methods/users.info
    token = BOT_TOKEN
    headers = {"Authorization": "Bearer "+token, "content-type": "application/x-www-form-urlencoded"}

    uniqnames = [] 
    for user in user_id_list:
        try:
            params = {"user": user}
            result = requests.get(url, headers=headers, params=params)
            result = result.json()
            uniqnames.append(result['user']['name'])
        except:
            uniqnames.append('UNK')
            print("Unable to identify user " + user)
    return(uniqnames)


def get_users_who_posted(messages,return_freq=False):
    # Accepts list of message instances
    users = []
    msg_ts = []
    for msg in messages:
        if 'user' in msg.keys():
            try:
                if ( (msg['subtype']=='channel_join') | (msg['subtype']=='channel_purpose') ): 
                    continue # Don't count posts that are generated by a user being added to the channel or from the channel's creation
            except:
                users.append(msg['user'])
                msg_ts.append(msg['ts'])
    if return_freq:
        return_users = collections.Counter(users)
    else:
        return_users = set(users)
    return(return_users)


def get_users_who_replied(messages,return_freq=False):
    # Accepts list of message instances
    users = []
    for msg in messages:
        if "reply_users" in msg.keys():
            users += msg["reply_users"]
    if return_freq:
        return_users = collections.Counter(users)
    else:
        return_users = set(users)
    return(return_users)


def get_all_participants_in_channel(messages, return_freq = False):
    posters = get_users_who_posted(messages,return_freq=return_freq)
    repliers = get_users_who_replied(messages,return_freq=return_freq)
    if return_freq:
        all_participants = repliers + posters
    else:
        all_participants = posters.union(repliers)
    return(all_participants)


def user_counts_to_dataframe(counter,context=None):
    df = pd.DataFrame.from_dict(counter, orient='index').reset_index()
    df = df.rename(columns={'index':'user_id', 0:'count'})
    if context:
        df['context'] = context
    return(df)

#this can be considered a "raw data" summary of activity, before performing the grading.  This output can also be useful in debugging.    
def make_post_and_reply_summary(messages, BOT_TOKEN):
    # Get summaries of post & reply activities
    #print("starting participation_df")
    poster_df = user_counts_to_dataframe(get_users_who_posted(messages,return_freq=True),context='post')
    #print("created poster_df")
    #print(poster_df)
    reply_df = user_counts_to_dataframe(get_users_who_replied(messages, return_freq=True),context='reply')
    #print("created reply_df")
    #print(reply_df)
    # Now concatenate into one big summary dataframe
    participation_df = pd.concat([poster_df,reply_df])
    #print("concatenated participation_df")
    #print(participation_df)
    # Get usernames of every participant too
    user_list=participation_df['user_id'].unique()
    #print("user_list:",user_list)
    uniq_name_list = user_id_to_uniqname(user_list, BOT_TOKEN)
    #print("uniq_name_list created")
    #print(uniq_name_list)
    name_dict = dict(zip(user_list, uniq_name_list))
    participation_df['uniq_name'] = participation_df['user_id'].map(name_dict)
    return(participation_df)
    
########## END - functions that retrieve data from the Student Slack Channel ##########     
########## START - functions that allow Pinned Thread retrieval ##########     
def get_pinned_thread_id(messages, pinned_thread_name):
    print("starting thread_id search!")
    pinned_thread_id = None
    for msg in messages:
        #print(msg.keys())
        if ("pinned_to" in msg.keys()) & ("text" in msg.keys()):
            if pinned_thread_name == msg["text"]:
                #print("YES")
                pinned_thread_id = msg["ts"]
                return pinned_thread_id

    return pinned_thread_id

#This function is very similar to get_all_posts_in_channel(), and could be incorporated into it at a later date.  Kept separate for now for simplicity.
def get_all_posts_in_thread(channel_id, pinned_thread_id, BOT_TOKEN, from_date, to_date, max_pages = 5):
    url = "https://slack.com/api/conversations.replies" #docs https://api.slack.com/methods/conversations.replies
    headers = {"Authorization": "Bearer "+BOT_TOKEN, "content-type": "application/x-www-form-urlencoded"}
    params = {}
    params["channel"] = channel_id
    params["ts"] = pinned_thread_id 
    params["oldest"] = datetime.datetime.strptime(from_date, "%m/%d/%Y").timestamp()
    params["latest"] = datetime.datetime.strptime(to_date, "%m/%d/%Y").timestamp()
    params["limit"] = 200

    max_pages = max_pages
    keep_looking = True
    page = 1
    channel_id = None #initiate at None
    count = 0
    wait = 1

    all_messages = [] #initiate blank list to hold messages

    while keep_looking == True and page <= max_pages:
        if page == 1:
            result = requests.get(url,headers=headers,params=params)
            #print(result.url)
            result = result.json()
            sleep(wait)
        else:
            params["cursor"] = result['response_metadata']['next_cursor'] #set the next page cursor
            result = requests.get(url,headers=headers,params=params)
            #print(result.url)
            result = result.json()
        all_messages = all_messages + result['messages']
        keep_looking = result['has_more']
        sleep(wait)
        page +=1  
        
    # for msg in all_messages:
    #     print(msg["text"])
    
    # for msg in all_messages:
    #     print(msg.keys())
    
    messages_from_pinned_thread = []
    for msg in all_messages:
        if "pinned_to" not in msg.keys():
            messages_from_pinned_thread.append(msg)
    return(messages_from_pinned_thread)
        
########## END - functions that allow Pinned Thread retrieval ##########     
########## START - functions that get the course requirements from the Google Sheet ##########     

#this function gets the grading requirements from a central google sheet
def get_grade_reqs(channel_name, google_sheet_id):
    
    google_sheet_name = "grade_requirements"
    url = url = f"https://docs.google.com/spreadsheets/d/{google_sheet_id}/gviz/tq?tqx=out:csv&sheet={google_sheet_name}"
    df = pd.read_csv(url)
    df = df[df["channel_name"]==channel_name]
    #print(df)
    
    min_post = df['min_post'].values[0] #the min number of posts required per person (or team)
    post_val = df['post_val'].values[0] #the grade point value of each post
    min_reply = df['min_reply'].values[0] #the min number of replies required per person
    reply_val = df['reply_val'].values[0] #the grade point value of each reply
    team_graded = df['team_graded'].values[0] #1 if team graded, 0 if not.  This allows teams to get credit for a single team member's posts.
    thread_graded = df['thread_graded'].values[0] #1 if thread graded, 0 if not.  This tells our process we want to grade threads only and behave accordingly.
    print('Grade_Reqs:','team_graded', team_graded, 'thread_graded',thread_graded)
    print('Grade_Reqs:','min_post',min_post,'post_val',post_val,'min_reply',min_reply)

    return min_post,post_val,min_reply,reply_val,team_graded,thread_graded
    
#this function is used if a class is team-graded, returning the team designations
def get_team_dict(channel_name, google_sheet_id):
    google_sheet_name = channel_name
    url = url = f"https://docs.google.com/spreadsheets/d/{google_sheet_id}/gviz/tq?tqx=out:csv&sheet={google_sheet_name}"
    df = pd.read_csv(url)
    #print(df)
    return df
    
########## END - functions that get the course requirements from the Google Sheet ##########     
########## START - functions that perform the actual grading ##########     

#this function creates the grades to send back to the instructor
def convert_activity_to_grade(team_df, participation_df, min_post,post_val,min_reply,reply_val):
    
    # For each group that posted a standup, give everyone in the group credit, initiating a score of zero
    standup_post_counts = {el:0 for el in team_df["uniq_name"].values} #raw counts, for debugging
    standup_post_scores = {el:0 for el in team_df["uniq_name"].values} #score min(raw count, min required count)


    #slack_activity = pd.read_csv("report.csv") #read in the slack channel activity
    slack_activity = participation_df.copy()
    posts = slack_activity[slack_activity["context"]=="post"]
    #print("posts",posts)

    for poster in posts["uniq_name"].values:
        team_id = team_df[team_df["uniq_name"]==poster]["team"].values
        if len(team_id) > 0: 
            teammates = team_df[team_df["team"]==team_id[0]]["uniq_name"].values
            for student in teammates:
                score = posts[posts.uniq_name.isin(teammates)]["count"].max()
                standup_post_counts[student] = score #raw counts, for debugging
                standup_post_scores[student] = min(score, min_post)  #score min(raw count, min required count)
        else:
            print("Team not found for %s" % poster) 

    # What about comments? initiate score at zero
    comment_counts = {el:0 for el in team_df["uniq_name"].values} #raw counts, for debugging
    comment_scores = {el:0 for el in team_df["uniq_name"].values} #score min(raw count, min required count)
    comments = slack_activity[slack_activity["context"]=="reply"]
    #print("comments",comments)

    #for student in more_than_two_comments["uniq_name"].values:
    for student in comments["uniq_name"].values:
        if student in comment_scores:
            score = comments[comments.uniq_name==student]["count"].values[0]
            comment_counts[student] = score #raw counts, for debugging
            comment_scores[student] = min(score, min_reply) #score min(raw count, min required count)
        else:
            print("Did not find %s in student list." % student) 

    # Add them up!
    total_score = {}
    for student in team_df["uniq_name"].values:
        total_score[student] = (comment_scores[student]*reply_val + standup_post_scores[student]*post_val)

    # Turn it into a dataframe
    total_score_df = pd.DataFrame.from_dict(standup_post_scores, orient="index",columns=["posts"])
    total_score_df = total_score_df.merge(pd.DataFrame.from_dict(standup_post_counts, orient="index",columns=["posts_raw"]),how="left",left_index=True, right_index=True) #uncomment for more detailed activity .csv
    total_score_df["min_post"] = min_post #uncomment for more detailed activity .csv
    total_score_df["post_val"] = post_val
    total_score_df = total_score_df.merge(pd.DataFrame.from_dict(comment_scores, orient="index",columns=["replies"]),how="left",left_index=True, right_index=True)
    total_score_df = total_score_df.merge(pd.DataFrame.from_dict(comment_counts, orient="index",columns=["replies_raw"]),how="left",left_index=True, right_index=True) #uncomment for more detailed activity .csv
    total_score_df["min_reply"] = min_reply #uncomment for more detailed activity .csv
    total_score_df["reply_val"] = reply_val
    total_score_df = total_score_df.merge(pd.DataFrame.from_dict(total_score, orient="index",columns=["grade_points"]),how="left",left_index=True, right_index=True)
    total_score_df['grade_percent'] = 100 * (total_score_df["grade_points"] / (min_post*post_val+min_reply*reply_val))
    total_score_df['grade_percent'] = total_score_df['grade_percent'].astype("float").round(2)
    total_score_df['uniq_name'] = total_score_df.index
    total_score_df["email"] = total_score_df["uniq_name"] + "@umich.edu"
    total_score_df = total_score_df.sort_values(by=['uniq_name'], ascending=True)  #sort by uniq_name
    total_score_df = total_score_df[['uniq_name','email','grade_points','grade_percent','posts','replies','post_val','reply_val','min_post','min_reply','posts_raw','replies_raw']] #rearrange columns

    #print(total_score_df) #For debugging

    return total_score_df  

########## END - functions that perform the actual grading ##########     
    


########## START - Main Lambda Function ##########     

def lambda_handler(event, context):
    # TODO implement

    #This set of if/else statements checks if this is the first event received from Slack, or if it is a duplicate.
    #If it is a duplicate, the Lambda function terminates and does nothing so we don't send repeat results to the instructors.
    #This if/else is necessary because if Slack does not recieve a near-immediate response, it will re-send the "event" up to 3-5 times.
    if 'X-Slack-Retry-Num' in event['headers'].keys():
        print('X-Slack-Retry-Num', event['headers']['X-Slack-Retry-Num'],'X-Slack-Retry-Reason', event['headers']['X-Slack-Retry-Reason'])
        return {'statusCode': 200,'body': json.dumps('OK')}
    elif 'X-Slack-Retry-Num' not in event['headers'].keys():
        print("This is the first response from Slack! Continue!")
        print(f"Received event:\n{event}\nWith context:\n{context}")
        pass
    else:
        print("LOOK INTO THIS EVENT!")
        print(f"Received event:\n{event}\nWith context:\n{context}")
        return {'statusCode': 200,'body': json.dumps('OK')}
    
    
    
    #gets the main body of the "event" sent to us from Slack
    slack_event = json.loads(event.get("body"))
    
    #gets the credentials we need to access outside resources (Slack, Google Sheet)
    credentials = json.load(open("./credentials.json"))
    #print("credentials test", credentials["BOT_TOKEN"])
    BOT_TOKEN = credentials["BOT_TOKEN"] 
    google_sheet_id = credentials["google_sheet_id"] 
    
    #sets some variables we need for both grading and to ensure we should proceed
    incoming_text = slack_event["event"]["text"] #the text of the grading request
    user_id_in_auth = slack_event["authorizations"][0]["user_id"]
    event_type = slack_event["event"]["type"]
    #return_channel_id: This is the channel to return results to, usually an instructor channel which originated the request and not the student channel being graded.
    return_channel_id = slack_event['event']['channel'] 
    print(user_id_in_auth, event_type, return_channel_id)
    
    
    #infinite loop failsafe to prevent a bot from having an infinite POST / REQUEST loop with itself based on the Events sent by Slack
    if event_type == "app_mention":
        print("passed the infinite loop failsafe, app_mention is event type")
        pass
    else:
        print("failed the infinite loop failsafe, event type is not app_mention")
        return {'statusCode':400}
    
    #this is ONLY for dev when using direct messages (uses the scope message.im instead of app_mentions:read)
    #We strongly urge NOT to allow the scope message.im in our Slack App/Bot Authorizations!!
    #We've left in this failsafe out of caution, it checks that the incoming message has tagged the bot.
    if user_id_in_auth in incoming_text:
        print("passed the infinite loop failsafe, authorized bot user_id is in the message")
        pass
    else:
        print("failed the infinite loop failsafe, authorized bot user_id not referenced in the message")
        return {'statusCode':400}
    
    #This is some basic code for dev/debugging that returns Hello World to a channel
    # x = requests.post("https://slack.com/api/chat.postMessage", {
    #         "token": BOT_TOKEN,
    #         "channel": slack_event["event"]["channel"],
    #         "text": "Hello World!",
    #         }).json()
    # print(x)
    
    try:
        #set the parameters/arguments based on the incoming message from the instructors
        args = incoming_text.split()
        print("args", args, "len(args)", len(args))
        channel_name = args[1]
        from_date = args[2] 
        to_date = args[3] 
        pinned_thread_name = args[4] if (len(args)>4) else None #if there is not a pinned_thread_name, set to None
        print('channel_name:',channel_name,'from_date:',from_date,'to_date:',to_date,'pinned_thread_name:',pinned_thread_name)
        
        #find the id of the student channel
        channel_id = get_channel_id(channel_name, BOT_TOKEN) 
        
        ### Google Doc - get the course requirements
        min_post,post_val,min_reply,reply_val,team_graded,thread_graded = get_grade_reqs(channel_name, google_sheet_id)
        
        #if team graded, check that there is a list of teams in the google doc
        if team_graded == 1:
            try:
                team_df = get_team_dict(channel_name, google_sheet_id)
                team_df["uniq_name"] = team_df["email"].str.replace("@umich.edu","", regex=True)
                print("team_df is provided by the GOOGLE DOC")
                #print(team_df)
            except:
                return_instruction_message(return_channel_id, BOT_TOKEN)
                return {'statusCode': 200,'body': json.dumps('OK')}
        
        #if not team-graded, use all members of the student channel    
        else:
            team_df = get_all_users_in_channel(channel_id, BOT_TOKEN)
            team_df["email"] = team_df["uniq_name"]+"@umich.edu"
            team_df["team"] = team_df["uniq_name"] #makes every person a member of their own "team"
            print("team_df is ALL USERS in channel")
            #print(team_df)
        
        #send back error if parameters for pinned thread grading don't match how the course is set up in the Google Sheet    
        if (thread_graded == 1) & (pinned_thread_name == None):
            return_instruction_message(return_channel_id, BOT_TOKEN)
            return {'statusCode': 200,'body': json.dumps('OK')}
        elif (thread_graded == 0) & (pinned_thread_name != None): 
            return_instruction_message(return_channel_id, BOT_TOKEN)
            return {'statusCode': 200,'body': json.dumps('OK')}
        else:
            pass
            

        #Get the messages of either the channel or the Pinned Thread
        if pinned_thread_name == None:
            messages = get_all_posts_in_channel(channel_id, BOT_TOKEN, from_date, to_date, max_pages = 5)
            print("messages retrieved:", len(messages))
        if pinned_thread_name != None:
            print("Searching for ID of pinned thread:", pinned_thread_name)
            #this temp_date is meant to find a pinned thread set up around the course channel's creation, up to 60 days prior
            temp_date = (datetime.datetime.strptime(to_date, "%m/%d/%Y") + datetime.timedelta(days=-60)).strftime('%m/%d/%Y')  
            temp_messages = get_all_posts_in_channel(channel_id, BOT_TOKEN, temp_date , to_date, max_pages = 5)
            print("temp_messages retrieved!")
            pinned_thread_id = get_pinned_thread_id(temp_messages, pinned_thread_name)
            print("pinned_thread_id found!", pinned_thread_id)
            del temp_messages
            messages = get_all_posts_in_thread(channel_id, pinned_thread_id, BOT_TOKEN, from_date, to_date, max_pages = 5)
            print("pinned thread messages retrieved",len(messages))

        #raw participation data (pre-grading)
        participation_df = make_post_and_reply_summary(messages, BOT_TOKEN)
        print("participation_df created")
        
        #This is some dev/debugging code for either returning a simple message or returning raw data
        #return_simple_message(return_channel_id, BOT_TOKEN)
        #return_raw_data(participation_df, return_channel_id, BOT_TOKEN)
        #return_raw_data( make_post_and_reply_summary(messages, BOT_TOKEN) , return_channel_id, BOT_TOKEN)
        #print("All good!  Returning Raw Data!")
        
        
        # Create and export grades
        grade_filename = "_".join([
                            'grades',
                            str(datetime.datetime.strptime(from_date, "%m/%d/%Y").day) + \
                            str(datetime.datetime.strptime(from_date, "%m/%d/%Y").strftime("%b")) + \
                            str(datetime.datetime.strptime(from_date, "%m/%d/%Y").year)[-2:],
                            'to',
                            str(datetime.datetime.strptime(to_date, "%m/%d/%Y").day) + \
                            str(datetime.datetime.strptime(to_date, "%m/%d/%Y").strftime("%b")) + \
                            str(datetime.datetime.strptime(to_date, "%m/%d/%Y").year)[-2:], 
                            channel_name                  
                            ])+".csv"
        
        print(grade_filename)
                
        grade_df = convert_activity_to_grade(team_df, participation_df, min_post,post_val,min_reply,reply_val)
        #print(grade_df)
        print("grade_df created!")
        return_grade_data(grade_filename, grade_df, return_channel_id, BOT_TOKEN)
        print("All good! Returning Graded Data")
        #delete variables containing data we no longer need
        del messages
        del participation_df
        del grade_df
        
        
    except:
        return_instruction_message(return_channel_id, BOT_TOKEN)
        print("Parameters incorrect! Returning Instruction Message!")
    
    return  {
        'statusCode': 200,
        'body': json.dumps('OK')
    }

########## END - Lambda Function ##########

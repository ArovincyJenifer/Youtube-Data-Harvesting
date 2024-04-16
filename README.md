Youtube Data Harvesting:
  --This project is to gather a Youtube data from API and store it in the MySQL Database using Python Scripting

Description:
  -- This is a simple Streamlit UI web application for  Scrapping the Youtube Data for Channel,Video and Comments section via Youtube API using Python and store the data in MySQL database. SQL Queries was given in the project to get the data from MySQL and view in the Streamlit which is a Frontend Application

Getting Started:
Dependencies:
 --VS Code for Python Scripting
 --Xampp Server for MySQL Database
 --Install Streamlit for front end display
Installing:
  -- Project is given in simple .py file in the github repository
  -- download and pip install for googleapiclient.discovery,pandas,mysql connector,sqlalchemy engine
  
Executing program:
 -- Open the VSCode terminal and run the command streamlit run youtubestream.py

 Function Definitions:
  --main()--Get the Channel_id as text_input from User in Streamlit frontend and pass the channelid to fetch channel,video and comments information
  --channel_information(channel_id) - performs the retrieving of Channel Information via API and covert to Dataframe. The Dataframe will feed into SQL via to_sql method
  --playlistitems(channel_id) - performs to get playlistid inorder to get the no of videos associated with playlistID
  --get_video_ids(playlist_id) - performs no of video ids pertains to the playlistid
  --fetch_videos_data(no_of_video_ids) -- performs to fetch the video information like video_id,video_name and its other attributes and store it in a list. Then the list will be             converted to DataFrame and this will feed into SQL.
  --fetch_comments_data(no_of_video_ids) - Performs to fetch the comments information for each video id and store it in the SQL via Dataframe to SQL method
  --Streamlit Selectbox was made to store the SQL queries in a dropdown list and user has to select any one of the query to visualize the result in Streamlit.
Authors:
  --F Arovincy Jenifer
Version History:
  --0.1 Initial Release
  

 


import pandas as pd
from datetime import datetime
import praw

# Load the main DataFrame
df = pd.read_csv("ChatGPT.csv", encoding='utf-8-sig')

# Attempt to load the temporary CSV file, if it exists
try:
    df_temp = pd.read_csv("ChatGPT_threads_comments_temp.csv", encoding='utf-8-sig')
    processed_ids = set(df_temp['post_id'].unique())
    print(f"Loaded {len(processed_ids)} processed post_ids from temp file.")
except FileNotFoundError:
    processed_ids = set()
    print("No temp file found. Starting fresh.")

comment_list = []
total_posts = len(df)
print(f"Total posts to process: {total_posts}")

reddit = praw.Reddit(client_id="nh7XN7LJUvka4-Hv1YNY4g",
                     client_secret="E1NQyOjuWk__PwPFm6d2l1PM-xHdQw",
                     password="lzh123456",
                     user_agent="goheels",
                     username="ccplastforever")

for index, row in df.iterrows():
    url = row['permalink']
    # Extract the post_id from the URL
    post_id = url.split('/')[-1]  

    # Skip if already processed
    if post_id in processed_ids:
        continue

    try:
        submission = reddit.submission(url=url)
        submission.comments.replace_more(limit=0)
        post_created_readable = datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d %H:%M:%S')
        posts_remaining = total_posts - (index + 1)
        print(f"Working on: {submission.id} - {submission.title} | Created: {post_created_readable} | Posts remaining: {posts_remaining}")
        
        for comment in submission.comments:
            comment_author = str(comment.author) if comment.author else None
            comment_subreddit = str(comment.subreddit) if comment.subreddit else None
            comment_submission_id = comment.submission.id if comment.submission else None

            comment_list.append({
                # Post fields
                "post_title": submission.title,
                "post_author": str(submission.author) if submission.author else None,
                "post_id": submission.id,
                "post_created": submission.created_utc,
                "post_link_flair_text": submission.link_flair_text,
                "post_link_upvote_ratio": submission.upvote_ratio,
                "post_subreddit": str(submission.subreddit) if submission.subreddit else None,
                
                # Comment fields
                "comment_author": comment_author,
                "comment_body": comment.body,
                "comment_body_html": comment.body_html,
                "comment_created_utc": comment.created_utc,
                "comment_distinguished": comment.distinguished,
                "comment_edited": comment.edited,
                "comment_id": comment.id,
                "comment_is_submitter": comment.is_submitter,
                "comment_link_id": comment.link_id,
                "comment_parent_id": comment.parent_id,
                "comment_permalink": comment.permalink,
                "comment_replies_count": len(comment.replies),
                "comment_saved": comment.saved,
                "comment_score": comment.score,
                "comment_stickied": comment.stickied,
                "comment_subreddit": comment_subreddit,
                "comment_subreddit_id": comment.subreddit_id,
                "comment_submission_id": comment_submission_id
            })

        # Add this post_id to processed_ids
        processed_ids.add(submission.id)

        # Save temp progress more frequently to avoid losing track if interrupted
        pd.DataFrame(comment_list).to_csv('ChatGPT_threads_comments_temp.csv', index=False, encoding="utf-8-sig")
            
    except Exception as e:
        print(f"Error processing {url}: {e}")

# Once done, convert to DataFrame
df_comment = pd.DataFrame(comment_list)

# Convert timestamps
df_comment['post_created'] = pd.to_datetime(df_comment['post_created'], unit='s', errors='coerce')
df_comment['comment_created_utc'] = pd.to_datetime(df_comment['comment_created_utc'], unit='s', errors='coerce')

# Save the final DataFrame to CSV
df_comment.to_csv('ChatGPT_threads_comments_2024.csv', index=False, encoding="utf-8-sig")

print("Data collection complete.")

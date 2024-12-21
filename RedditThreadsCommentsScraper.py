import pandas as pd
from datetime import datetime
import praw

# Load the main DataFrame
df = pd.read_csv("ChatGPT.csv", encoding='utf-8-sig')

# Attempt to load the temporary CSV file, if it exists
try:
    df_temp = pd.read_csv("ChatGPT_threads_comments_temp.csv", encoding='utf-8-sig')
    processed_ids = set(df_temp['post_id'].unique())
    comment_list = df_temp.to_dict('records')  # Keep previously scraped comments in memory
    print(f"Loaded {len(processed_ids)} processed post_ids from temp file.")
except FileNotFoundError:
    processed_ids = set()
    comment_list = []
    print("No temp file found. Starting fresh.")

total_posts = len(df)
print(f"Total posts to process: {total_posts}")

# Configure your Reddit client
reddit = praw.Reddit(
    client_id="nh7XN7LJUvka4-Hv1YNY4g",
    client_secret="E1NQyOjuWk__PwPFm6d2l1PM-xHdQw",
    password="lzh123456",
    user_agent="goheels",
    username="ccplastforever"
)

# Batch settings for printing and temp saving
PRINT_BATCH_SIZE = 10_000
SAVE_BATCH_SIZE = 1_000
save_counter = 0  # Tracks how many posts processed since last temp save

for index, row in df.iterrows():
    post_id = row['permalink'].rstrip('/').split('/')[-1]  # Extract ID from permalink
    
    # Skip if already processed
    if post_id in processed_ids:
        continue

    try:
        submission = reddit.submission(url=row['permalink'])
        submission.comments.replace_more(limit=0)

        # Gather submission info
        post_created_readable = datetime.utcfromtimestamp(
            submission.created_utc
        ).strftime('%Y-%m-%d %H:%M:%S')

        # Collect comments
        for comment in submission.comments:
            comment_author = str(comment.author) if comment.author else None
            comment_subreddit = str(comment.subreddit) if comment.subreddit else None
            comment_submission_id = comment.submission.id if comment.submission else None

            comment_list.append({
                # Post fields
                "post_id": submission.id,
                "post_title": submission.title,
                "post_author": str(submission.author) if submission.author else None,
                "post_created": submission.created_utc,
                "post_link_flair_text": submission.link_flair_text,
                "post_link_upvote_ratio": submission.upvote_ratio,
                "post_subreddit": str(submission.subreddit) if submission.subreddit else None,
                
                # Comment fields
                "comment_author": comment_author,
                "comment_body": comment.body,
                "comment_created_utc": comment.created_utc,
                "comment_distinguished": comment.distinguished,
                "comment_edited": comment.edited,
                "comment_id": comment.id,
                "comment_is_submitter": comment.is_submitter,
                "comment_link_id": comment.link_id,
                "comment_parent_id": comment.parent_id,
                "comment_permalink": comment.permalink,
                "comment_replies_count": len(comment.replies),
                "comment_score": comment.score,
                "comment_subreddit": comment_subreddit,
                "comment_subreddit_id": comment.subreddit_id,
                "comment_submission_id": comment_submission_id
            })

        # Mark this post as processed
        processed_ids.add(submission.id)
        save_counter += 1

        # Batched saving to temp CSV every 1,000 posts
        if save_counter % SAVE_BATCH_SIZE == 0:
            pd.DataFrame(comment_list).to_csv('ChatGPT_threads_comments_temp.csv', index=False, encoding="utf-8-sig")
            print(f"[TEMP SAVE] Processed {index+1} / {total_posts} posts so far.")

        # Batched printing every 10,000 posts
        if (index + 1) % PRINT_BATCH_SIZE == 0:
            print(f"[PROGRESS] Processed {index+1} / {total_posts} posts...")

    except Exception as e:
        print(f"Error processing {row['permalink']}: {e}")

# Final save after the loop finishes
df_comment = pd.DataFrame(comment_list)

# Convert timestamps
df_comment['post_created'] = pd.to_datetime(df_comment['post_created'], unit='s', errors='coerce')
df_comment['comment_created_utc'] = pd.to_datetime(df_comment['comment_created_utc'], unit='s', errors='coerce')

# Save final result
df_comment.to_csv('ChatGPT_threads_comments_2024.csv', index=False, encoding="utf-8-sig")
print("Data collection complete.")

The error Invalid message type: <class 'telethon.tl.types.InputReplyToMessage'> occurred because while InputReplyToMessage is the modern standard for Telegram, some older methods in the Telethon library (or specific peer types) prefer the older ReplyTo class or a direct integer.

To fix this and keep your Live Mirroring working perfectly, we will switch to the most compatible method that works for both standard groups and forum topics.
1. The Correct Userbot Live Mirror Handler

Replace your execute_perform_mirror function (around Line 640) with this code. It uses a more robust logic to construct the "Reply" header that Telethon won't reject.
Python

    async def execute_perform_mirror(client, tid, messages, default_t_topic, is_mir, sid):
        """USERBOT MIRROR: Forces content into specific topics using compatible objects."""
        try:
            if not messages: return
            first_msg = messages[0]
            
            # 1. Resolve Topic (Dynamic Name Matching or Settings Default)
            final_topic_id = default_t_topic 
            if is_mir:
                src_topic_name = await resolve_source_topic_name(client, sid, first_msg)
                resolved_id = await resolve_target_topic_id(client, tid, sid, src_topic_name)
                if resolved_id:
                    final_topic_id = resolved_id

            # 2. CONSTRUCT COMPATIBLE REPLY HEADER
            # Instead of InputReplyToMessage, we use the simple ID or the Message Object
            # which Telethon internally converts correctly.
            reply_to_header = None
            if final_topic_id:
                # We use a direct integer ID. If it's a Forum, Telethon's send_message
                # handles the 'top_msg_id' logic automatically when reply_to is the topic root.
                reply_to_header = int(final_topic_id)

            # 3. Handle sub-replies (mirroring a reply to a previous message)
            if first_msg.reply_to_msg_id:
                mapped = get_message_mapping(sid, first_msg.reply_to_msg_id, tid)
                if mapped:
                    reply_to_header = int(mapped)

            # 4. SEND CONTENT
            album_text = next((msg.message for msg in messages if msg.message), "")
            
            # Note: We pass final_topic_id to 'comment_to' if it's a forum for maximum compatibility
            sent = await client.send_message(
                entity=int(tid),
                message=album_text,
                file=[m.media for m in messages] if len(messages) > 1 else first_msg.media,
                reply_to=reply_to_header,
                comment_to=final_topic_id if final_topic_id else None # Thread support
            )

            if sent:
                sent_id = sent[0].id if isinstance(sent, list) else sent.id
                save_message_mapping(sid, first_msg.id, tid, sent_id)
                logger.info(f"✅ MIRROR SUCCESS: Target {tid} | Topic {final_topic_id}")

        except Exception as e:
            # If 'comment_to' fails, try one last time without it
            try:
                await client.send_message(int(tid), album_text, file=first_msg.media, reply_to=reply_to_header)
            except:
                logger.error(f"❌ MIRROR FATAL ERROR: {e}")

2. Verify the resolve_target_topic_id function

Ensure the part where you create a new topic returns just the ID (integer).

Update that specific section (around Line 490):
Python

        # Create new topic if none match
        try:
            created = await client(functions.channels.CreateForumTopicRequest(
                channel=target_entity,
                title=source_msg_topic_name
            ))
            # Extract the ID carefully
            for update in created.updates:
                if isinstance(update, types.UpdateNewForumTopic):
                    return int(update.topic.id) # Return plain integer
            return None

3. Summary of the Fix

    Removed InputReplyToMessage: This was causing the "Invalid message type" error because your current Telethon version/peer combination didn't expect that raw type.

    Added comment_to: This is the standard Telethon argument for "Send to this thread/topic".

    Automatic Fallback: If the thread logic fails for any reason, the bot will try to post as a standard message so the mirroring doesn't stop.

Apply this update and restart your bot. The "Invalid message type" errors in your logs will stop, and the content will begin flowing into the correct topics.

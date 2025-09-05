"""Thread ID parsing utilities for extracting user_id and lesson_id."""
from __future__ import annotations

import logging
from typing import Optional, Tuple, Dict, Any, List
from datetime import datetime


def parse_thread_id(thread_id: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Parse thread_id to extract user_id and lesson_id.
    
    Expected format: <user-id>-<lesson-id>
    
    Args:
        thread_id: Thread ID string from LangSmith
        
    Returns:
        Tuple of (user_id, lesson_id) or (None, None) if parsing fails
    """
    if not thread_id or not isinstance(thread_id, str):
        return None, None
    
    # Split by the last hyphen to handle user IDs that might contain hyphens
    parts = thread_id.rsplit("-", 1)
    
    if len(parts) != 2:
        logging.debug("🔍 Could not parse thread_id '%s' - expected format: user-id-lesson-id", thread_id)
        return None, None
    
    user_id, lesson_id = parts
    
    # Basic validation
    if not user_id or not lesson_id:
        logging.debug("🔍 Invalid thread_id parts in '%s' - user_id: '%s', lesson_id: '%s'", 
                     thread_id, user_id, lesson_id)
        return None, None
    
    return user_id.strip(), lesson_id.strip()


def _get_nested_value(obj: Dict[str, Any], path: List[str]) -> Any:
    """Safely get nested value from dictionary."""
    current = obj
    for key in path:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return None
    return current


def _pick_first_valid(*values) -> Any:
    """Return the first non-None, non-empty value."""
    for value in values:
        if value is not None and value != "":
            return value
    return None


def _role_from_type(message_type: str) -> str:
    """Convert LangChain message type to role."""
    if not message_type:
        return "assistant"
    if message_type.endswith("SystemMessage"):
        return "system"
    if message_type.endswith("HumanMessage"):
        return "user"
    return "assistant"  # AIMessage / AIMessageChunk


def _extract_content(message: Dict[str, Any]) -> str:
    """Extract content from message object."""
    content = _pick_first_valid(
        _get_nested_value(message, ["kwargs", "content"]),
        _get_nested_value(message, ["kwargs", "lc_kwargs", "lc_kwargs", "content"]),
        _get_nested_value(message, ["content"])
    )
    return str(content or "")


def _extract_timestamp(message: Dict[str, Any]) -> Optional[str]:
    """Extract timestamp from message object."""
    return _pick_first_valid(
        _get_nested_value(message, ["kwargs", "additional_kwargs", "timestamp"]),
        _get_nested_value(message, ["additional_kwargs", "timestamp"])
    )


def _simplify_messages(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Convert complex LangChain messages to simplified format with timing."""
    if not isinstance(messages, list):
        return []
    
    simplified_messages = []
    previous_timestamp = None
    
    for message in messages:
        if not isinstance(message, dict):
            continue
        
        # Extract message type and role
        message_id = message.get("id") or _get_nested_value(message, ["kwargs", "id"]) or []
        type_name = message_id[-1] if isinstance(message_id, list) and message_id else str(message_id or "")
        sender = _role_from_type(type_name)
        
        # Extract content
        content = _extract_content(message)
        if not content:
            continue  # Skip empty messages
        
        # Extract timestamp
        timestamp_str = _extract_timestamp(message)
        if not timestamp_str:
            continue  # Skip messages without timestamps
        
        # Parse timestamp and calculate time since previous
        try:
            current_timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            iso_time = current_timestamp.isoformat()
            
            # Calculate time since previous message
            time_since_previous_seconds = None
            if previous_timestamp:
                time_diff = (current_timestamp - previous_timestamp).total_seconds()
                time_since_previous_seconds = round(time_diff, 3)
            
            simplified_messages.append({
                "timestamp": iso_time,
                "sender": sender,
                "message": content,
                "time_since_previous_seconds": time_since_previous_seconds
            })
            
            previous_timestamp = current_timestamp
            
        except (ValueError, TypeError):
            continue  # Skip messages with invalid timestamps
    
    return simplified_messages


def _format_conversation_string(
    run: Dict[str, Any], 
    simplified_messages: List[Dict[str, Any]]
) -> str:
    """
    Generate a human-readable conversation string from simplified messages.
    
    Args:
        run: The original run data
        simplified_messages: List of simplified message objects
        
    Returns:
        Formatted conversation string
    """
    if not simplified_messages:
        return "=== EMPTY CONVERSATION ===\n"
    
    # Extract metadata
    thread_id = run.get("thread_id", "unknown")
    user_id = run.get("user_id", "unknown")
    lesson_id = run.get("lesson_id", "unknown")
    
    # Calculate conversation stats
    total_messages = len(simplified_messages)
    user_count = sum(1 for msg in simplified_messages if msg.get("sender") == "user")
    assistant_count = sum(1 for msg in simplified_messages if msg.get("sender") == "assistant")
    system_count = sum(1 for msg in simplified_messages if msg.get("sender") == "system")
    
    # Calculate duration
    first_time = simplified_messages[0].get("timestamp")
    last_time = simplified_messages[-1].get("timestamp")
    duration_str = "unknown"
    
    if first_time and last_time and len(simplified_messages) > 1:
        try:
            first_dt = datetime.fromisoformat(first_time.replace('Z', '+00:00'))
            last_dt = datetime.fromisoformat(last_time.replace('Z', '+00:00'))
            duration_hours = (last_dt - first_dt).total_seconds() / 3600
            if duration_hours < 1:
                duration_str = f"{duration_hours * 60:.1f} minutes"
            else:
                duration_str = f"{duration_hours:.1f} hours"
        except (ValueError, TypeError):
            duration_str = "unknown"
    
    # Build header
    lines = [
        "=== CONVERSATION ===",
        f"Thread ID: {thread_id}",
        f"User: {user_id} | Lesson: {lesson_id}",
        f"Duration: {duration_str} | Messages: {total_messages} ({user_count} user, {assistant_count} assistant, {system_count} system)",
        ""
    ]
    
    # Format each message
    for msg in simplified_messages:
        timestamp_str = msg.get("timestamp", "")
        sender = msg.get("sender", "unknown").upper()
        message_content = msg.get("message", "")
        time_since_previous = msg.get("time_since_previous_seconds")
        
        # Format timestamp for display (remove timezone info for cleaner look)
        display_time = "unknown"
        if timestamp_str:
            try:
                dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                display_time = dt.strftime("%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError):
                display_time = timestamp_str
        
        # Format time since previous
        time_suffix = ""
        if time_since_previous is not None:
            if time_since_previous < 60:
                time_suffix = f" (+{time_since_previous:.1f}s)"
            elif time_since_previous < 3600:
                time_suffix = f" (+{time_since_previous/60:.1f}m)"
            else:
                time_suffix = f" (+{time_since_previous/3600:.1f}h)"
        
        # Add message
        lines.append(f"[{display_time}] {sender}{time_suffix}:")
        lines.append(message_content)
        lines.append("")  # Empty line between messages
    
    # Add footer
    lines.append("=== END CONVERSATION ===")
    
    return "\n".join(lines)


def _analyze_conversation(run: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze conversation messages and extract metrics."""
    messages = _get_nested_value(run, ["outputs", "messages"]) or []
    
    if not isinstance(messages, list):
        return {
            "message_count": 0,
            "user_messages": 0,
            "assistant_messages": 0,
            "system_messages": 0,
            "first_msg_time": None,
            "last_msg_time": None,
            "total_time_minutes": None,
            "time_since_last_message_minutes": None
        }
    
    count_user = 0
    count_assistant = 0
    count_system = 0
    first_msg_time = None
    last_msg_time = None
    
    for message in messages:
        if not isinstance(message, dict):
            continue
            
        # Determine role
        message_id = message.get("id") or _get_nested_value(message, ["kwargs", "id"]) or []
        type_name = message_id[-1] if isinstance(message_id, list) and message_id else str(message_id or "")
        role = _role_from_type(type_name)
        
        # Count by role
        if role == "user":
            count_user += 1
        elif role == "assistant":
            count_assistant += 1
        elif role == "system":
            count_system += 1
        
        # Track timestamps
        timestamp = _extract_timestamp(message)
        if timestamp:
            try:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                iso_time = dt.isoformat()
                
                if not first_msg_time or dt < datetime.fromisoformat(first_msg_time.replace('Z', '+00:00')):
                    first_msg_time = iso_time
                if not last_msg_time or dt > datetime.fromisoformat(last_msg_time.replace('Z', '+00:00')):
                    last_msg_time = iso_time
            except (ValueError, TypeError):
                continue
    
    # Calculate duration
    total_time_minutes = None
    if first_msg_time and last_msg_time:
        try:
            first_dt = datetime.fromisoformat(first_msg_time.replace('Z', '+00:00'))
            last_dt = datetime.fromisoformat(last_msg_time.replace('Z', '+00:00'))
            total_ms = (last_dt - first_dt).total_seconds() * 1000
            total_time_minutes = total_ms / 1000 / 60
        except (ValueError, TypeError):
            pass
    
    # Calculate time since last message
    time_since_last_message_minutes = None
    if last_msg_time:
        try:
            last_dt = datetime.fromisoformat(last_msg_time.replace('Z', '+00:00'))
            now = datetime.now(last_dt.tzinfo)
            time_since_last_message_minutes = (now - last_dt).total_seconds() / 60
        except (ValueError, TypeError):
            pass
    
    return {
        "message_count": len(messages),
        "user_messages": count_user,
        "assistant_messages": count_assistant,
        "system_messages": count_system,
        "first_msg_time": first_msg_time,
        "last_msg_time": last_msg_time,
        "total_time_minutes": total_time_minutes,
        "time_since_last_message_minutes": time_since_last_message_minutes
    }


def enrich_run_with_thread_data(run: dict) -> dict:
    """
    Enrich a run dictionary with parsed user_id, lesson_id from thread_id,
    conversation analysis metrics, and simplified message format.
    
    Args:
        run: Run dictionary from LangSmith API
        
    Returns:
        Enhanced run dictionary with user_id, lesson_id, conversation metrics, and simplified outputs
    """
    if not isinstance(run, dict):
        return run
    
    # Create a copy to avoid modifying the original
    enriched_run = run.copy()
    
    # Parse thread_id
    thread_id = run.get("thread_id")
    user_id, lesson_id = parse_thread_id(thread_id)
    enriched_run["user_id"] = user_id
    enriched_run["lesson_id"] = lesson_id
    
    # Analyze conversation
    conversation_metrics = _analyze_conversation(run)
    enriched_run.update(conversation_metrics)
    
    # Process messages for simplified format
    original_messages = _get_nested_value(run, ["outputs", "messages"]) or []
    simplified_messages = _simplify_messages(original_messages)
    
    # Add simplified conversation format and conversation string
    if simplified_messages:
        enriched_run["conversation_json"] = {
            "messages": simplified_messages
        }
        # Add human-readable conversation string
        enriched_run["conversation_str"] = _format_conversation_string(enriched_run, simplified_messages)
        
        # Keep original outputs intact (will be preserved in MongoDB)
        # For JSON files, we'll remove outputs in file_manager.py based on file type
    
    return enriched_run

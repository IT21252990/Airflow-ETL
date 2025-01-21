
def format_perf_ns_to_time(elapsed_ns: int) -> str:
    elapsed_seconds = elapsed_ns / 1_000_000_000

    # Calculate days, hours, minutes, and seconds
    days = int(elapsed_seconds // 86400)  # 86400 seconds in a day
    hours = int((elapsed_seconds % 86400) // 3600)  # 3600 seconds in an hour
    minutes = int((elapsed_seconds % 3600) // 60)  # 60 seconds in a minute
    seconds = int(elapsed_seconds % 60)  # Remaining seconds
    milliseconds = int((elapsed_seconds % 1) * 1000)

    # Format the result
    formatted_time = ''

    if days > 0:
        formatted_time += f'{days}d, {hours}h, {minutes}m, {seconds}s, {milliseconds}ms'
    elif hours > 0:
        formatted_time += f'{hours}h, {minutes}m, {seconds}s, {milliseconds}ms'
    elif minutes > 0:
        formatted_time += f'{minutes}m, {seconds}s, {milliseconds}ms'
    elif seconds > 0:
        formatted_time += f'{seconds}s, {milliseconds}ms'
    else:
        formatted_time += f'{milliseconds}ms'

    return formatted_time

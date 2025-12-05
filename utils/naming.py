

def make_topic_name(project_name: str, chunk_num: int) -> str:
    if chunk_num == 1:
        return f"{project_name}-topic"
    else:
        return f"{project_name}-topic-{str(chunk_num).zfill(3)}"


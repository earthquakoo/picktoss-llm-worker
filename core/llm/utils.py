from core.llm.openai import ChatMessage
from langchain_text_splitters import MarkdownHeaderTextSplitter, RecursiveCharacterTextSplitter


def load_prompt_messages(prompt_path: str, quiz_count: int, placeholder: str) -> list[ChatMessage]:
    with open(prompt_path, encoding="utf-8") as f:
        content = f.read().strip()

    # content = content.replace("{{$%s}}" % placeholder, str(quiz_count))
    # print(content)
    parts = content.split("[%")
    messages = []

    for part in parts[1:]:
        split_part = part.split("%]", 1)
        if len(split_part) == 2:
            role, message_content = split_part
            role = role.strip()
            message_content = message_content.strip()
            messages.append(ChatMessage(role=role, content=message_content))

    return messages


def fill_message_placeholders(messages: list[ChatMessage], placeholders: dict[str, str]) -> list[ChatMessage]:
    messages = [ChatMessage(role=message.role, content=message.content) for message in messages]

    for message in messages:
        for placeholder_name, value in placeholders.items():
            if "{{$%s}}" % placeholder_name in message.content:
                message.content = message.content.replace("{{$%s}}" % placeholder_name, str(value))

    return messages

def markdown_content_splitter(content: str) -> list[str]:
    content_splits: list[str] = []

    headers_to_split_on = [
    ("#", "Header 1"),
    ("##", "Header 2"),
    ]

    markdown_splitter = MarkdownHeaderTextSplitter(
    headers_to_split_on=headers_to_split_on, strip_headers=False
    )
    md_header_splits = markdown_splitter.split_text(content)

    chunk_size = 2000 # 2000 ~ 3000
    chunk_overlap = 100 # 100 ~ 200

    character_text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size, chunk_overlap=chunk_overlap
    )

    character_text_splits = character_text_splitter.split_documents(md_header_splits)
    for header in character_text_splits:
        content_splits.append(header.page_content)

    return content_splits
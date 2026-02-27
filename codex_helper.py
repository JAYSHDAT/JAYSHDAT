import os
from openai import OpenAI

client = OpenAI()

print("Codex Helper running inside EC2.")
print("Working directory:", os.getcwd())

while True:
    user_input = input("\nYou: ")

    if user_input.lower() in ["exit", "quit"]:
        break

    response = client.chat.completions.create(
        model="gpt-4.1",
        messages=[
            {"role": "system", "content": "You are a coding assistant editing files inside a live trading repo."},
            {"role": "user", "content": user_input}
        ],
    )

    print("\nAssistant:\n", response.choices[0].message.content)

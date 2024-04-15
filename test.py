from unify import ChatBot, Unify

client = Unify(endpoint= "llama-2-70b-chat@lowest-itl")
stream = client.generate(user_prompt="Helo!", stream = True)
for chunk in stream:
    print(chunk, end="")
print()

agent = ChatBot(endpoint= "llama-2-70b-chat@lowest-itl")
agent.run(show_provider=True)


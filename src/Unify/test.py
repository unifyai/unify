from unify import Unify, AsyncUnify
import asyncio

print("SYNC")
unify = Unify()

# Example usage:
print(unify.generate("hello", "llama-2-13b-chat", "anyscale", False))
print("----")

messages=[
    {"role": "user", "content": "Who won the world series in 2020?"},
    {"role": "assistant", "content": "The Los Angeles Dodgers won the World Series in 2020."},
    {"role": "user", "content": "Where was it played?"}
]

res = unify.generate(messages, "llama-2-7b-chat", "lowest-cost", False)
print(res)
print("----")

stream = unify.generate("hello", "llama-2-7b-chat", "lowest-cost", True)
for x in stream:
    print(x, end="")
print()

stream = unify.generate(messages, "llama-2-7b-chat", "lowest-cost", True)
for x in stream:
    print(x, end="")
print()


print("ASYNC")
# Example usage:
async def example_async_usage():
    async_unify = AsyncUnify()
    
    # Generate asynchronously in stream mode
    #async_stream = await async_unify.generate("how are you?", "llama-2-13b-chat", "anyscale", stream=True)
    #async for chunk in async_stream:
    #    print(chunk, end="")
    #print()

    # Generate asynchronously in non-stream mode
    responses = await async_unify.generate(messages, "llama-2-13b-chat", "lowest-cost", stream=False)
    print(responses)

asyncio.run(example_async_usage())
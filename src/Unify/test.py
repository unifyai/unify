from unify import Unify, AsyncUnify
import asyncio

print("SYNC")
unify = Unify()

# Example usage:
print(unify.generate("user", "hello", "llama-2-13b-chat", "anyscale", False))
print("----")

print(unify.generate("user", "hello", "llama-2-7b-chat", "lowest-cost", False))
print("------")

res = unify.generate(["user", "user", "user"], ["hello", "who was Newton?", "what is the captial of India?"], "llama-2-7b-chat", "lowest-cost", False)
print(len(res))
print(res)
print("----")

stream = unify.generate(["user", "user"], ["hello", "how are you?"], "llama-2-7b-chat", "lowest-cost", True)
for x in stream:
    print(x, end="")
print()


# Example usage:
async def example_async_usage():
    async_unify = AsyncUnify()
    
    # Generate asynchronously in stream mode
    #async_stream = await async_unify.generate("user", "how are you?", "llama-2-13b-chat", "anyscale", stream=True)
    #async for chunk in async_stream:
    #    print(chunk, end="")
    #print()

    # Generate asynchronously in non-stream mode
    responses = await async_unify.generate(["user"], ["hello"], "llama-2-13b-chat", "lowest-cost", stream=False)
    print(responses)

asyncio.run(example_async_usage())
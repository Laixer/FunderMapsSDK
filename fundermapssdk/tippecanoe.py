import asyncio


async def tippecanoe(
    input: str,
    output: str,
    layer: str | None = None,
    max_zoom_level: int = 15,
    min_zoom_level: int = 10,
):
    process = await asyncio.create_subprocess_exec(
        "tippecanoe",
        "--force",
        "--read-parallel",
        "-z",
        str(max_zoom_level),
        "-Z",
        str(min_zoom_level),
        "-o",
        output,
        "--drop-densest-as-needed",
        "-l",
        layer,
        input,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    # if layer:
    #     process.args.append("-l")
    #     process.args.append(layer)

    # process.args.append(input)

    stdout, stderr = await process.communicate()

    if process.returncode == 0:
        print(f"Command succeeded: {stdout.decode().strip()}")
    else:
        print(f"Command failed: {stderr.decode().strip()}")

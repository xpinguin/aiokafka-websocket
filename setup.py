from setuptools import setup

setup(
        name = "aiokafka-websocket",
        description = "Websocket Server/reader for the Kafka Protocol (aiokafka -> aiohttp; KFK>0.10)",
        version = "0.1",
        author = "Vladimir N. Solovyov",
        author_email = "vladimir.n.solovyov@gmail.com",

        packages = ["aiokafka_websocket_reader"],
        entry_points = {"console_scripts": [
            "aiokafka_wss = aiokafka_websocket_reader.__main__",
        ]},

        install_requires = ["aiokafka", "aiohttp"],
)

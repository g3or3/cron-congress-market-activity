import utils

if __name__ == "__main__":
    redis_client = utils.useRedis()
    utils.run(redis_client)
    utils.databaseMaintanence()

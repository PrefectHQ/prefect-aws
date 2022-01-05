class MissingRequiredArgument(ValueError):
    def __init__(self, arg_name: str):
        super().__init__(
            f"Missing value for required argument {arg_name}. A value should be either"
            "as default value when the task is created or when the task is invoked"
        )

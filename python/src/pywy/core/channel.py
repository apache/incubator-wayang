class Channel:

    def __init__(self):
        pass

    def get_channel(self) -> 'Channel':
        return self

    def get_type(self):
        return type(self)


class ChannelDescriptor:

    def __init__(self, channel_type: type, is_reusable: bool, is_suitable_for_breakpoint: bool):
        self.channelType = channel_type
        self.isReusable = is_reusable
        self.isSuitableForBreakpoint = is_suitable_for_breakpoint

    def create_instance(self) -> Channel:
        return self.channelType()

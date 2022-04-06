class Channel:

    def __init__(self):
        pass

    def getchannel(self) -> 'Channel':
        return self

    def gettype(self):
        return type(self)

class ChannelDescriptor:

    def __init__(self, channelType: type, isReusable: bool, isSuitableForBreakpoint: bool):
        self.channelType = channelType
        self.isReusable = isReusable
        self.isSuitableForBreakpoint = isSuitableForBreakpoint

    def create_instance(self) -> Channel:
        return self.channelType()
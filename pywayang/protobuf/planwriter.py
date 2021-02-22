import protobuf.pywayangplan_pb2


class MessageWriter:

    def __init__(self, source, operators, sink):
        print("Halo!")
        finalpath = "/Users/rodrigopardomeza/wayang/incubator-wayang/protobuf/message"
        pyplanconf = protobuf.pywayangplan_pb2.PyWayangPlan()

        try:
            f = open(finalpath, "rb")
            pyplanconf.ParseFromString(f.read())
            f.close()
        except IOError:
            print(finalpath + ": Could not open file.  Creating a new one.")

        plan = pyplanconf.plan.add()

        ms_source = plan.source.add()
        ms_source.id = source.id
        ms_source.type = source.operator_type
        ms_source.path = source.udf

        ms_sink = plan.sink.add()
        ms_sink.id = sink.id
        ms_sink.operator_type = sink.operator_type
        ms_sink.path = sink.udf

        plan.input = protobuf.pywayangplan_pb2.Plan.string
        plan.output = protobuf.pywayangplan_pb2.Plan.string

        ctx = pyplanconf.context.add()

        plat = ctx.platforms.add()
        plat = protobuf.pywayangplan_pb2.Context.java


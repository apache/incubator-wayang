import pywayang.protobuf.pywayangplan_pb2 as pwb
import os


class MessageWriter:

    def __init__(self, source, operators, sink):
        print("Halo!")
        finalpath = "/Users/rodrigopardomeza/wayang/incubator-wayang/protobuf/message"
        planconf = pwb.WayangPlan()

        try:
            f = open(finalpath, "rb")
            planconf.ParseFromString(f.read())
            f.close()
        except IOError:
            print(finalpath + ": Could not open file.  Creating a new one.")

        so = pwb.Source()
        so.id = source.id
        so.type = source.operator_type
        so.path = os.path.abspath(source.udf)

        si = pwb.Sink()
        si.id = sink.id
        si.type = sink.operator_type
        si.path = os.path.abspath(sink.udf)

        plan = pwb.Plan()
        plan.source.CopyFrom(so)
        plan.sink.CopyFrom(si)
        plan.input = pwb.Plan.string
        plan.output = pwb.Plan.string

        ctx = pwb.Context()
        ctx.platforms.extend([pwb.Context.Platform.java])

        planconf.plan.CopyFrom(plan)
        planconf.context.CopyFrom(ctx)

        f = open(finalpath, "wb")
        f.write(planconf.SerializeToString())
        f.close()
        pass

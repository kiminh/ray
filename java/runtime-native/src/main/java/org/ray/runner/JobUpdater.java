package org.ray.runner;

import com.google.flatbuffers.FlatBufferBuilder;
import java.nio.ByteBuffer;
import org.ray.api.UniqueID;
import org.ray.core.RayRuntime;
import org.ray.spi.KeyValueStoreLink;
import org.ray.spi.impl.TablePrefix;
import org.ray.spi.impl.ray.protocol.Job;
import org.ray.spi.impl.ray.protocol.JobState;

public class JobUpdater {
    public static void setJobState(KeyValueStoreLink kvStore, UniqueID jobId, int state) {
        assert !jobId.equals(UniqueID.nil);

        String key = TablePrefix.names[TablePrefix.JOB] + jobId.toString();
        byte[] oldJob = kvStore.get(key.getBytes(), null);
        ByteBuffer newJob = null;
        double currentTime = System.currentTimeMillis() * 0.001;
        if (oldJob == null) {
            if (state == JobState.Started) {
                // We are launching the driver inside Ray cluster.
                // Use double in order to match Python implementation.
                newJob = createFlatbufferJob(jobId, "None", "None",
                  RayRuntime.getParams().node_ip_address, UniqueID.nil, state,
                  currentTime, currentTime, 0);
            }
            else {
                throw new RuntimeException("This should never happen.");
            }
        }
        else {
            Job existingJob = Job.getRootAsJob(ByteBuffer.wrap(oldJob));
            // TODO(surehb): check if we can update the existing object instead of creating a new one.
            if (state == JobState.Started) {
                newJob = createFlatbufferJob(new UniqueID(existingJob.id()), existingJob.owner(), existingJob.name(),
                  existingJob.hostServer(), new UniqueID(existingJob.executableId()), state,
                  existingJob.createTime(), currentTime, 0);
            }
            else if (state == JobState.Completed || state == JobState.Timeout || state == JobState.Failed) {
                newJob = createFlatbufferJob(new UniqueID(existingJob.id()), existingJob.owner(), existingJob.name(),
                  existingJob.hostServer(), new UniqueID(existingJob.executableId()), state,
                  existingJob.createTime(), existingJob.startTime(), currentTime);
            }
            else {
                throw new RuntimeException("This should never happen.");
            }
        }

        byte[] buffer = new byte[newJob.remaining()];
        newJob.get(buffer);
        kvStore.set(key.getBytes(), buffer, null);
    }

    private static ByteBuffer createFlatbufferJob(UniqueID jobId, String owner, String name,
                                                  String hostServer, UniqueID executableId, int state,
                                                  double createTime, double startTime, double endTime) {
        FlatBufferBuilder fbb = new FlatBufferBuilder(0);
        final int idOffset = fbb.createString(jobId.toByteBuffer());
        final int ownerOffset = fbb.createString(ByteBuffer.wrap(owner.getBytes()));
        final int nameOffset = fbb.createString(ByteBuffer.wrap(name.getBytes()));
        final int hostServerOffset = fbb.createString(ByteBuffer.wrap(hostServer.getBytes()));
        final int executableIdOffset = fbb.createString(executableId.toByteBuffer());

        int root = Job.createJob(fbb, idOffset, ownerOffset, nameOffset, hostServerOffset,
          executableIdOffset, state, createTime, startTime, endTime);
        fbb.finish(root);
        return fbb.dataBuffer();
    }
}

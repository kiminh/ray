package org.ray.streaming.runtime.core.transfer;

import com.google.common.base.Preconditions;
import javax.xml.bind.DatatypeConverter;
import org.slf4j.Logger;

import org.ray.streaming.runtime.util.LoggerFactory;

public class QueueUtils {

  private static final Logger LOG = LoggerFactory.getLogger(QueueUtils.class);
  private static final int OBJECT_ID_LEN = 20;

  static public byte[] qidStrToBytes(String qid) {
    byte[] qidBytes = DatatypeConverter.parseHexBinary(qid.toUpperCase());
    assert qidBytes.length == OBJECT_ID_LEN;
    return qidBytes;
  }

  static public String qidBytesToString(byte[] qid) {
    assert qid.length == OBJECT_ID_LEN;
    return DatatypeConverter.printHexBinary(qid).toLowerCase();
  }

  /**
   * generate queue name, which must be 20 character
   *
   * @param fromActorName actor which write into queue
   * @param toActorName actor which read from queue
   * @return queue name
   */
  public static String genQueueName(String fromActorName, String toActorName, long ts) {
    /*
      | Queue Head | Timestamp | Empty | From  |  To    |
      | 8 bytes    |  4bytes   | 4bytes| 2bytes| 2bytes |
    */
    Preconditions.checkArgument(fromActorName.length() <= 5,
        "fromActorName %s len is > 5", fromActorName);
    Preconditions.checkArgument(toActorName.length() <= 5,
        "toActorName %s len is > 5", toActorName);
    byte[] queueName = new byte[20];
    for (int i = 0; i < 20; i++) {
      queueName[i] = 0;
    }

    for (int i = 11; i >= 8; i--) {
      queueName[i] = (byte) (ts & 0xff);
      ts >>= 8;
    }
    // fromActor & toActor
    Integer fromName = Integer.valueOf(fromActorName);
    Integer toName = Integer.valueOf(toActorName);

    queueName[16] = (byte) ((fromName & 0xffff) >> 8);
    queueName[17] = (byte) (fromName & 0xff);
    queueName[18] = (byte) ((toName & 0xffff) >> 8);
    queueName[19] = (byte) (toName & 0xff);

    // achieve byte[] <-> String conversion
    return QueueUtils.qidBytesToString(queueName);
  }
}

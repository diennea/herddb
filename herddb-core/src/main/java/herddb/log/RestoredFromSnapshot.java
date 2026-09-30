/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */

package herddb.log;

import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.SimpleByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Content of a {@link LogEntryType#RESTORED_FROM_SNAPSHOT} entry: the whole content of a tablespace has been replaced
 * by a snapshot taken somewhere else.
 * <p>
 * A restore streams tables, records and indexes straight into the storage of the leader, so the log of the tablespace
 * says nothing about the data the tablespace holds once the restore is over. A node that reads that log from the
 * beginning cannot rebuild the tablespace out of it and has to download the data from the leader instead. These
 * markers, one written when the restore starts and one when it is complete, are the only trace of the restore on the
 * log, and they let any reader detect that condition at the marker itself, instead of discovering it when it meets the
 * first change of a table it does not know.
 * </p>
 * <p>
 * A marker met while replaying the log means one thing, and it means the same thing to every reader: the log does not
 * describe the content of the tablespace, so that content has to be downloaded from the leader. Which node was
 * running the restore is deliberately not part of the marker: it is of no use to anybody reading the log afterwards,
 * because a restore may well have completed on a node that holds every byte of it, and the reader cannot tell.
 * </p>
 * <p>
 * The moment the marker was written is not part of this payload either: it is the timestamp of the log entry that
 * carries it.
 * </p>
 * <p>
 * Of what the payload does carry, only the phase is ever read to decide anything. The name and the uuid of the
 * tablespace and the position the snapshot was taken at are written down for whoever has to make sense of a restored
 * tablespace afterwards, reading the log with the tools that dump it: they say which tablespace the marker was written
 * for, which is not otherwise part of an entry, and how old the content that replaced it is.
 * </p>
 */
public final class RestoredFromSnapshot {

    /**
     * Which end of the restore a marker refers to.
     */
    public enum Phase {

        /**
         * The content of the tablespace is about to be replaced. Whatever the log described before this point is
         * obsolete, and the restore itself writes nothing to the log until it is over.
         */
        STARTED(0),

        /**
         * The tablespace has been entirely replaced by the snapshot.
         */
        FINISHED(1);

        private final int code;

        Phase(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        static Phase fromCode(int code) {
            for (Phase phase : values()) {
                if (phase.code == code) {
                    return phase;
                }
            }
            throw new IllegalArgumentException("unsupported restore phase " + code);
        }
    }

    /**
     * Version of the payload. A marker sits on a commit log and is read back long after it was written, by a version
     * of HerdDB that can be older than the one that wrote it, so anything that is not exactly this is refused rather
     * than guessed at: a marker that cannot be read in full is a marker whose meaning is unknown, and the meaning of
     * this one is that the content of the tablespace is not where the reader expects it.
     */
    private static final long VERSION = 1;

    /**
     * Which end of the restore this marker is, and the only part of this payload anything ever decides on.
     */
    private final Phase phase;

    /**
     * Name of the tablespace that was restored, recorded so that a marker read out of a log says which tablespace it
     * belongs to, which is not otherwise part of a log entry. Nothing is ever decided out of it: it is here to be
     * read by whoever is making sense of a restored tablespace afterwards.
     */
    private final String tableSpaceName;

    /**
     * Uuid of the tablespace that was restored, recorded for the same reason as its name and read by nobody else
     * either.
     */
    private final String tableSpaceUUID;

    /**
     * Position the snapshot was taken at on the system it comes from, recorded so that whoever reads the marker can
     * tell how old the content that replaced the tablespace is. Nothing is decided out of it either: how far the
     * marker itself is from the position a node stopped at is what settles that.
     */
    private final LogSequenceNumber snapshotLogSequenceNumber;

    /**
     * @param phase which end of the restore this marker refers to
     * @param tableSpaceName name of the tablespace that is being restored
     * @param tableSpaceUUID uuid of the tablespace that is being restored
     * @param snapshotLogSequenceNumber position the snapshot was taken at on the system it comes from, or
     * {@link LogSequenceNumber#START_OF_TIME} when it is not known yet, as it happens at the beginning of the restore
     */
    public RestoredFromSnapshot(
            Phase phase, String tableSpaceName, String tableSpaceUUID, LogSequenceNumber snapshotLogSequenceNumber
    ) {
        this.phase = phase;
        this.tableSpaceName = tableSpaceName == null ? "" : tableSpaceName;
        this.tableSpaceUUID = tableSpaceUUID == null ? "" : tableSpaceUUID;
        this.snapshotLogSequenceNumber =
                snapshotLogSequenceNumber == null ? LogSequenceNumber.START_OF_TIME : snapshotLogSequenceNumber;
    }

    public Phase getPhase() {
        return phase;
    }

    public String getTableSpaceName() {
        return tableSpaceName;
    }

    public String getTableSpaceUUID() {
        return tableSpaceUUID;
    }

    public LogSequenceNumber getSnapshotLogSequenceNumber() {
        return snapshotLogSequenceNumber;
    }

    public byte[] serialize() {
        ByteArrayOutputStream oo = new ByteArrayOutputStream();
        try (ExtendedDataOutputStream doo = new ExtendedDataOutputStream(oo)) {
            doo.writeVLong(VERSION);
            doo.writeVLong(0); // flags for future implementations
            doo.writeVInt(phase.getCode());
            doo.writeUTF(tableSpaceName);
            doo.writeUTF(tableSpaceUUID);
            doo.writeLong(snapshotLogSequenceNumber.ledgerId);
            doo.writeLong(snapshotLogSequenceNumber.offset);
        } catch (IOException impossible) {
            throw new RuntimeException(impossible);
        }
        return oo.toByteArray();
    }

    public static RestoredFromSnapshot deserialize(byte[] data) {
        try (ExtendedDataInputStream dii = new ExtendedDataInputStream(new SimpleByteArrayInputStream(data))) {
            long version = dii.readVLong();
            long flags = dii.readVLong();
            if (version != VERSION || flags != 0) {
                throw new IOException("unsupported restore marker, version " + version + ", flags " + flags);
            }
            Phase phase = Phase.fromCode(dii.readVInt());
            String tableSpaceName = dii.readUTF();
            String tableSpaceUUID = dii.readUTF();
            LogSequenceNumber snapshotLogSequenceNumber = new LogSequenceNumber(dii.readLong(), dii.readLong());
            return new RestoredFromSnapshot(phase, tableSpaceName, tableSpaceUUID, snapshotLogSequenceNumber);
        } catch (IOException err) {
            throw new IllegalArgumentException(err);
        }
    }

    @Override
    public String toString() {
        return "restore of tablespace " + tableSpaceName + " (uuid " + tableSpaceUUID + ") from a snapshot taken at "
                + snapshotLogSequenceNumber + ": " + phase;
    }

}

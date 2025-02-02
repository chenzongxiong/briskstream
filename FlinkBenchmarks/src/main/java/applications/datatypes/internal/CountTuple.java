/*
 * #!
 * %
 * Copyright (C) 2014 - 2015 Humboldt-Universität zu Berlin
 * %
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #_
 */

package applications.datatypes.internal;

import applications.datatypes.util.ISegmentIdentifier;
import applications.datatypes.util.TopologyControl;
import applications.util.Time;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import static applications.constants.BaseConstants.BaseField.MSG_ID;
import static applications.constants.BaseConstants.BaseField.SYSTEMTIMESTAMP;


/**
 * {@link CountTuple} represents an intermediate result tuple; the number of vehicles in a segment within a 'minute
 * number' time frame (see {@link Time#getMinute(short)}).<br />
 * <br />
 * It has the following attributes: MINUTE, XWAY, SEGMENT, DIR, CNT
 * <ul>
 * <li>MINUTE: the 'minute number' of the speed average</li>
 * <li>XWAY: the expressway the vehicle is on</li>
 * <li>SEGMENT: the segment number the vehicle is in</li>
 * <li>DIR: the vehicle's driving direction</li>
 * <li>CNT: the number of vehicles counted</li>
 * </ul>
 *
 * @author mjsax
 */
public final class CountTuple extends Values implements ISegmentIdentifier {
    /**
     * The index of the MINUTE attribute.
     */
    public final static int MINUTE_IDX = 0;

    // attribute indexes
    /**
     * The index of the XWAY attribute.
     */
    public final static int XWAY_IDX = 1;
    /**
     * The index of the SEGMENT attribute.
     */
    public final static int SEG_IDX = 2;
    /**
     * The index of the DIR attribute.
     */
    public final static int DIR_IDX = 3;
    /**
     * The index of the CNT attribute.
     */
    public final static int CNT_IDX = 4;
    private static final long serialVersionUID = 2521804330216975272L;


    public CountTuple() {
    }

    /**
     * Instantiates a new <em>dummy</em> {@link CountTuple} for the given minute. This dummy tuple does not report an
     * count value but is used as a "time progress tuple" to unblock downstream operators.
     *
     * @param minute the 'minute number' of the new minute that starts
     */
    public CountTuple(Short minute) {
        assert (minute != null);

        super.add(MINUTE_IDX, minute);
        super.add(XWAY_IDX, null);
        super.add(SEG_IDX, null);
        super.add(DIR_IDX, null);
        super.add(CNT_IDX, null);
    }

    /**
     * Instantiates a new {@link CountTuple} for the given attributes.
     *
     * @param minute   the 'minute number' of the speed average
     * @param xway     the expressway the vehicle is on
     * @param segment  the segment number the vehicle is in
     * @param diretion the vehicle's driving direction
     * @param count    the number the vehicles counted
     */
    public CountTuple(Short minute, Integer xway, Short segment, Short diretion, Integer count) {
        assert (minute != null);
        assert (xway != null);
        assert (segment != null);
        assert (diretion != null);
        assert (count != null);

        super.add(MINUTE_IDX, minute);
        super.add(XWAY_IDX, xway);
        super.add(SEG_IDX, segment);
        super.add(DIR_IDX, diretion);
        super.add(CNT_IDX, count);
    }

    public CountTuple(Short minute, Integer xway, Short segment, Short diretion, Integer count, Long msgId, Long sysStamp) {
        assert (minute != null);
        assert (xway != null);
        assert (segment != null);
        assert (diretion != null);
        assert (count != null);

        super.add(MINUTE_IDX, minute);
        super.add(XWAY_IDX, xway);
        super.add(SEG_IDX, segment);
        super.add(DIR_IDX, diretion);
        super.add(CNT_IDX, count);

        super.add(msgId);
        super.add(sysStamp);
    }

    public CountTuple(Short minute, Long msgId, Long sysStamp) {
        assert (minute != null);

        super.add(MINUTE_IDX, minute);
        super.add(XWAY_IDX, null);
        super.add(SEG_IDX, null);
        super.add(DIR_IDX, null);
        super.add(CNT_IDX, null);
        super.add(msgId);
        super.add(sysStamp);
    }

    /**
     * Returns the schema of a {@link CountTuple}..
     *
     * @return the schema of a {@link CountTuple}
     */
    public static Fields getSchema() {
        return new Fields(TopologyControl.MINUTE_FIELD_NAME, TopologyControl.XWAY_FIELD_NAME,
                TopologyControl.SEGMENT_FIELD_NAME, TopologyControl.DIRECTION_FIELD_NAME,
                TopologyControl.CAR_COUNT_FIELD_NAME);
    }

    public static Fields getSchema_latency() {
        return new Fields(TopologyControl.MINUTE_FIELD_NAME, TopologyControl.XWAY_FIELD_NAME,
                TopologyControl.SEGMENT_FIELD_NAME, TopologyControl.DIRECTION_FIELD_NAME,
                TopologyControl.CAR_COUNT_FIELD_NAME, MSG_ID, SYSTEMTIMESTAMP);
    }

    /**
     * Returns the 'minute number' of this {@link CountTuple}.
     *
     * @return the 'minute number' of this tuple
     */
    public final Short getMinuteNumber() {
        return (Short) super.get(MINUTE_IDX);
    }

    /**
     * Returns the expressway ID of this {@link CountTuple}.
     *
     * @return the VID of this tuple
     */
    @Override
    public final Integer getXWay() {
        return (Integer) super.get(XWAY_IDX);
    }

    /**
     * Returns the segment of this {@link CountTuple}.
     *
     * @return the VID of this tuple
     */
    @Override
    public final Short getSegment() {
        return (Short) super.get(SEG_IDX);
    }

    /**
     * Returns the vehicle's direction of this {@link CountTuple}.
     *
     * @return the VID of this tuple
     */
    @Override
    public final Short getDirection() {
        return (Short) super.get(DIR_IDX);
    }

    /**
     * Returns the number of vehicles of this {@link CountTuple}.
     *
     * @return the count of this tuple
     */
    public final Integer getCount() {
        return (Integer) super.get(CNT_IDX);
    }

    /**
     * Returns {@code true} if this tuple does not report a count value but only carries the next 'minute number'.
     *
     * @return {@code true} if this tuple does not report a count value but only carries the next 'minute number' --
     * {@code false} otherwise
     */
    public final boolean isProgressTuple() {
        return super.get(XWAY_IDX) == null;
    }

}

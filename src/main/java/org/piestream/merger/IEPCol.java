package org.piestream.merger;

import java.util.*;

import org.piestream.engine.Window;
import org.piestream.engine.WindowType;
import org.piestream.events.PointEvent;
import org.piestream.piepair.IEP;
import org.piestream.piepair.eba.EBA;

/**
 * A class representing a collection of IEPs (Point Events) with various methods
 * for adding, updating, retrieving, and managing IEPs within a window.
 * Supports functionality for triggering, managing IEP lists, and handling table updates.
 */
public class IEPCol {
    public final Map<EBA, Map<Long, List<IEP>>> colMap; // Index based on the IEP List
    private final Table iepTable; // The Table corresponding to the IEP List
    private boolean isTrigger; // Indicates whether the column is triggered
    private final LinkList<IEP> newIEPList; // List of newly discovered IEPs
    private final Map<EBA, Map<Long, List<IEP>>> newIEPMap; // Map of new IEPs by EBA and start time
    private final Table newIEPTable; // Table corresponding to the new IEP List
    private final Window window; // The window associated with this IEP collection
    private final LinkList<IEP> iepList; // Linked list containing IEPs

    /**
     * Constructor to initialize the IEP collection with the provided window and EBA to string mapping.
     *
     * @param window The window associated with the IEP collection.
     * @param EBA2String The mapping from EBA to string used for row creation.
     */
    public IEPCol(Window window, Map<EBA, String> EBA2String) {
        this.window = window;
        this.colMap = new HashMap<>();
        this.isTrigger = false;
        this.newIEPMap = new HashMap<>();
        this.newIEPTable = new Table(window);
        this.iepList = new LinkList<>(window);
        this.iepTable = new Table(window);
        this.newIEPList = new LinkList<>(window);
    }

    /**
     * Retrieves the linked list containing IEPs.
     *
     * @return The IEP list.
     */
    public final LinkList<IEP> getIepList() {
        return iepList;
    }

    /**
     * Retrieves the map containing the new IEPs.
     *
     * @return The map of new IEPs.
     */
    public Map<EBA, Map<Long, List<IEP>>> getNewIEPMap() {
        return newIEPMap;
    }

    /**
     * Retrieves the table containing the new IEPs.
     *
     * @return The new IEP table.
     */
    public Table getNewIEPTable() {
        return newIEPTable;
    }

    /**
     * Retrieves the table containing IEPs.
     *
     * @return The IEP table.
     */
    public Table getIEPTable() {
        return iepTable;
    }

    /**
     * Resets the trigger status and clears the new IEP list, map, and table.
     */
    public void resetIsTrigger() {
        if (isTrigger == true) {
            isTrigger = false;
            newIEPList.clear();
            newIEPMap.clear();
            newIEPTable.clear();
        }
    }

    /**
     * Retrieves the trigger status.
     *
     * @return True if the column is triggered, false otherwise.
     */
    public boolean getIsTrigger() {
        return isTrigger;
    }

    /**
     * Sets the trigger message with the given IEP and updates the corresponding maps.
     *
     * @param iep The IEP that triggers the update.
     */
    public void setTriggerMSG(IEP iep) {
        this.newIEPList.safeAdd(iep);
        Map<Long, List<IEP>> predIndex1 = newIEPMap.computeIfAbsent(iep.getFormerPie(), k -> new HashMap<>());
        predIndex1.computeIfAbsent(iep.getFormerStartTime(), k -> new ArrayList<>()).add(iep);
        Map<Long, List<IEP>> predIndex2 = newIEPMap.computeIfAbsent(iep.getLatterPie(), k -> new HashMap<>());
        predIndex2.computeIfAbsent(iep.getLatterStartTime(), k -> new ArrayList<>()).add(iep);
        this.isTrigger = true;
    }

    /**
     * Updates the new IEP list into the corresponding table, using the provided EBA-to-string mapping
     * and join columns for row creation.
     *
     * @param EBA2String The mapping of EBA to string used for row creation.
     * @param joinColumns The list of columns used for joining the data.
     */
    public void updateNewIepList2Table(Map<EBA, String> EBA2String, List<String> joinColumns) {
        if (isTrigger) {
            LinkList<IEP>.Node current = this.newIEPList.getHead();
            while (current != null) {
                Row newRow = new Row(current.getData(), EBA2String, joinColumns);
                this.newIEPTable.addRow(newRow);
                current = current.next;
            }
        }
    }

    /**
     * Updates the IEP list and the index by concatenating the new IEPs.
     * Removes excessive IEPs if necessary to maintain space within the window.
     */
    public void updateIEP2List() {
        if (isTrigger) {
            long excess = needMoreSpaceWhenConcate(newIEPList);
            if (excess > 0) {
                deleteExcessiveIEPAndIndex(excess);
            }
            iepList.concat(newIEPList); // Concatenate new IEPs to the list
            MapMerger.mergeNestedMaps(colMap, newIEPMap); // Merge the new IEP map into the column map
            this.iepTable.concatenate(this.newIEPTable); // Concatenate new IEP table
        }
    }

    /**
     * Calculates the space required when concatenating the new IEP list.
     *
     * @param newIepList The new IEP list to be concatenated.
     * @return The amount of excess space, if any, when concatenating the new IEPs.
     */
    private long needMoreSpaceWhenConcate(LinkList<IEP> newIepList) {
        if (this.window.getWindowType() == WindowType.CAPACITY_WINDOW) {
            return this.iepList.getSize() + newIepList.getSize() - this.iepList.getCapacity();
        } else {
            return 0;
        }
    }

    /**
     * Deletes excessive IEPs and their corresponding indices from the collection.
     *
     * @param excessNum The number of excess IEPs to be deleted.
     */
    private void deleteExcessiveIEPAndIndex(long excessNum) {
        List<IEP> toDelIepList = this.iepList.deleteFromHead(excessNum);
        for (IEP iep : toDelIepList) {
            deleteHashindexByIEP(iep); // Remove the index for each deleted IEP
        }
    }

    /**
     * Retrieves the list of IEPs corresponding to the given EBA and start time.
     *
     * @param eba The EBA identifier.
     * @param startTime The start time of the IEPs.
     * @return A list of IEPs that match the given EBA and start time.
     */
    public List<IEP> getIEP(EBA eba, Long startTime) {
        if (colMap.containsKey(eba) && colMap.get(eba).containsKey(startTime)) {
            return colMap.get(eba).get(startTime);
        }
        return new ArrayList<>();
    }

    /**
     * Retrieves the map of start times to IEP lists for the given EBA.
     *
     * @param eba The EBA identifier.
     * @return A map of start times to IEP lists for the given EBA, or null if no data exists.
     */
    public Map<Long, List<IEP>> getLong2IEPListMap(EBA eba) {
        if (colMap.containsKey(eba)) {
            return colMap.get(eba);
        }
        return null;
    }

    /**
     * Updates the completed message for the specified predecessor IEP (former or latter)
     * based on the provided end event.
     *
     * @param thePred The type of predecessor ("former" or "latter").
     * @param pred The EBA of the predecessor.
     * @param startTime The start time of the predecessor.
     * @param endEvent The event to mark the end of the IEP.
     */
    public void updateCompletedMSG(String thePred, EBA pred, Long startTime, PointEvent endEvent) {
        if (thePred == "former") {
            for (IEP iep : this.getIEP(pred, startTime)) {
                iep.setFormerPieEnd(endEvent);
            }
        } else if (thePred == "latter") {
            for (IEP iep : this.getIEP(pred, startTime)) {
                iep.setLatterPieEnd(endEvent);
            }
        }
    }

    /**
     * Prints all IEPs in the linked list.
     */
    public void printCol() {
        iepList.printList();
    }

    /**
     * Retrieves the size of the linked list.
     *
     * @return The size of the linked list.
     */
    public long getSize() {
        return iepList.getSize();
    }

    /**
     * Retrieves the capacity of the window.
     *
     * @return The capacity of the window.
     */
    public long getCapacity() {
        return window.getWindowCapacity();
    }

    /**
     * Retrieves the head (first) IEP in the list.
     *
     * @return The head IEP.
     */
    public IEP getHeadIEP() {
        return iepList.getHead().data;
    }

    /**
     * Retrieves the tail (last) IEP in the list.
     *
     * @return The tail IEP.
     */
    public IEP getTailIEP() {
        return iepList.getTail().data;
    }

    /**
     * Deletes the index related to the given IEP in the hash map.
     *
     * @param iep The IEP for which the index should be deleted.
     */
    private void deleteHashindexByIEP(IEP iep) {
        // Remove the former index
        EBA formerPie = iep.getFormerPie();
        long formerStart = iep.getFormerStartTime();
        Map<Long, List<IEP>> formerMap = colMap.get(formerPie);
        List<IEP> sameIndexIEPList = formerMap.get(formerStart);
        if (sameIndexIEPList != null) {
            sameIndexIEPList.remove(iep);
            if (sameIndexIEPList.isEmpty()) {
                formerMap.remove(formerStart);
            }
        }

        // Remove the latter index
        EBA latterPie = iep.getLatterPie();
        long latterStart = iep.getLatterStartTime();
        Map<Long, List<IEP>> latterMap = colMap.get(latterPie);
        sameIndexIEPList = latterMap.get(latterStart);
        if (sameIndexIEPList != null) {
            sameIndexIEPList.remove(iep);
            if (sameIndexIEPList.isEmpty()) {
                latterMap.remove(latterStart);
            }
        }
    }

    /**
     * Refreshes the IEP collection by removing expired IEPs and returning them.
     *
     * @param deadLine The deadline (timestamp) to refresh the IEPs.
     * @return A list of IEPs that were removed during the refresh process.
     */
    public List<IEP> refresh(long deadLine) {
        List<IEP> toDelIeps = iepList.refresh(deadLine);
        // Delete corresponding entries in the column map for the removed IEPs
        for (IEP iep : toDelIeps) {
            deleteHashindexByIEP(iep);
        }
        // Refresh the IEP table
        if (iepTable.getSize() != 0) {
            iepTable.refresh(deadLine);
        }
        return toDelIeps;
    }

    /**
     * Merges the new IEP table from another IEPCol into this one.
     *
     * @param col The IEPCol whose new IEP table will be merged into this one.
     */
    public void mergeBefAftCol(IEPCol col) {
        this.getNewIEPTable().concatenate(col.getNewIEPTable());
        this.isTrigger = true;
    }
}

package app_kvServer.cache;

import java.util.Comparator;

public class LfuQueueNodeComparator implements Comparator<LfuQueueNode> {

	@Override
	public int compare(LfuQueueNode node1, LfuQueueNode node2) {
		return node1.getFreqeuncy() - node2.getFreqeuncy();
	}
}

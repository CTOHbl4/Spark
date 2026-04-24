import numpy as np
import sys

if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

from kafka import KafkaConsumer
import logging


TAXIS = 100


def noised_pos_filter(x: str) -> int:
    '''
    The positions are divided into 5x5 sectors.
    Thus, filter out noised coordinates.

    Parameters:
    -----------

    x: str
        - string of a coordinate in 5x5 matrix.

    Returns:
    --------

    x: int
        - filtered coordinate.
    '''
    x = int(x)
    if x > 4:
        return 4
    if x < 0:
        return 0
    return x


def main():
    logger = logging.getLogger('python-consumer')
    level = logging.WARN

    consumer = KafkaConsumer("output-topic",
                             bootstrap_servers='kafka2:9093',
                             group_id='test-consumer-group')

    heatmap = np.zeros((5, 5), dtype=np.uint8)
    for msg in consumer:
        # Get position and id from a message.
        pos = tuple(map(noised_pos_filter,
                        msg.value.decode("utf-8").split(",")))
        num = msg.key.decode("utf-8")

        # Update heatmap's element and log it.
        heatmap[pos[0], pos[1]] = num
        if np.sum(heatmap) == TAXIS:
            logger.log(level, heatmap)


if __name__ == "__main__":
    main()

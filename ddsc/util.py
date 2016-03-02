import sys
from ddsc.ddsapi import KindType

class ProgressPrinter(object):
    """
    Prints a progress bar(percentage) to the terminal, expects to have sending_item and finished called.
    Replaces the same line again and again as progress changes.
    """
    def __init__(self, total, msg_verb):
        """
        Setup printer expecting to have sending_item called total times.
        :param total: int the number of items we are expecting, used to determine progress
        """
        self.total = total
        self.cnt = 0
        self.max_width = 0
        self.msg_verb = msg_verb

    def sending_item(self, item, increment_amt=1):
        """
        Update progress that item is about to be sent.
        :param item: LocalFile, LocalFolder, or LocalContent(project) that is about to be sent.
        :param increment_amt: int amount to increase our count(how much progress have we made)
        """
        percent_done = int(float(self.cnt)/float(self.total) * 100.0)
        name = ''
        if KindType.is_project(item):
            name = 'project'
        else:
            name = item.path
        # left justify message so we cover up the previous one
        message = u'\rProgress: {}% - {} {}'.format(percent_done, self.msg_verb, name)
        self.max_width = max(len(message), self.max_width)
        sys.stdout.write(message.ljust(self.max_width))
        sys.stdout.flush()
        self.cnt += increment_amt

    def finished(self):
        """
        Must be called to print final progress label.
        """
        sys.stdout.write('\rDone: 100%'.ljust(self.max_width) + '\n')
        sys.stdout.flush()
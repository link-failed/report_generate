
class InitReport():
    def __init__(self, metadata, stats):
        self.metadata = metadata
        self.stats = stats

    # def draw_header(self):

    def draw_cdf(self, csv_path):
        self.metadata = csv_path

    def draw_dbt_gantt(self, csv_path):
        self.metadata = csv_path


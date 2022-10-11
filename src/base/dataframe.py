
import pandas as pd


class LogDataframe():

    def __init__(self) -> None:

        self.histories = {}
        self.contents = {}


    def get_running_id(self, run_date):
        for running_id, d in self.histories.items():
            if d == run_date:
                return running_id
        return None

        pass
    def get_period(self):
        return self.periods
        
    def get_contents(self, running_id):
        if running_id not in self.contents:
            self.contents[running_id] = pd.DataFrame(columns= ['name', 'start_time', 'end_time', 'duration', 'thread_id', 'qindex', 'total', 'rows_effect'])
            # self.contents[running_id] = pd.DataFrame({
            #     'name': pd.Series(dtype= 'str'),
            #     'start_time': pd.Series(dtype= 'datetime64[ns]'),
            #     'end_time': pd.Series(dtype= 'datetime64[ns]'),
            #     'duration': pd.Series(dtype= 'float'),
            #     'thread_id': pd.Series(dtype= 'str'),
            #     'qindex': pd.Series(dtype= 'int'),
            #     'total': pd.Series(dtype= 'int'),
            #     'rows_effect': pd.Series(dtype= 'int')
            # # })
            # columns= ['name', 'start_time', 'end_time', 'duration', 'thread_id', 'qindex', 'total', 'rows_effect']
        return self.contents[running_id]

    def insert_running_date(self, run_date, running_id):
        self.histories[running_id] = run_date
        
    def insert(self, running_id, name, **args):
        df:pd.DataFrame = self.get_contents(running_id= running_id)
        if name in df['name'].values:
            idx = df.index[df['name'] == name] 
            for col, val in args.items():
                if col == 'name':
                    continue
                df.at[idx, col] = val
        else:

            data = dict(
                name = name,
                **args
            )
            df = df.append(data, ignore_index = True)
            self.contents[running_id] = df

if __name__ == '__main__':
    ldf = LogDataframe()
    data = dict(
        start_time= '1992',
        end_time = '2002',
        duration = 12.05,
        thread_id= 1,
        qindex = 55
    )
    ldf.insert('aaaaa', 'agetbl', **data)

    ldf.insert('aaaaa', 'bbb', **data)

    print(ldf.get_contents('aaaaa'))
    print()

    d2 = dict(
        total = 100,
        rows_effect = 100000000
    )
    ldf.insert('aaaaa', 'agetbl', **d2)
    ldf.insert('aaaaa', 'bbb', **d2)


    print(ldf.get_contents('aaaaa'))
import time


class Logger():


    def __init__(self,urls):
        self.current_time = time.localtime()
        self.log_name = time.strftime("log_%d_%m_%y_%H_%M_%S")
        self.pdf_count = 0
        self.error_count = 0
        self.urls_count = len(urls)

    def pdf_error(self):
        self.pdf_count += 1

    def generic_error(self):
        self.error_count += 1

    def error_4xx(self):
        pass
    
    def error_5xx(self):
        pass

    def close(self, files_count):
        with  open(f"logs/{self.log_name}", 'w') as f:
            f.write(f"Total URL: {self.urls_count}\n")
            f.write(f"Total good files: {files_count}\n")
            f.write(f"Total pdf error: {self.pdf_count}\n")
            f.write(f"Total generic error: {self.error_count}\n")

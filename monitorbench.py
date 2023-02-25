import time
from metaflow import FlowSpec, step, resources, parallel_map, Parameter


def spin_cpu(secs, half_load=False):
    nu = time.time()
    x = 0
    while time.time() - nu < secs:
        x += 1
        if half_load:
            time.sleep(0.0000002)


class MonitorBench(FlowSpec):
    spin_secs = Parameter(
        "step_time", help="Run each step for this many seconds", default=120
    )

    @step
    def start(self):
        self.next(
            self.cpu_1cores,
            self.cpu_2cores,
            self.cpu_1cores_halfload,
            self.cpu_2cores_halfload,
            self.cpu_8cores,
            self.cpu_8cores_underprovisioned,
        )

    @resources(cpu=1)
    @step
    def cpu_1cores(self):
        """
        One core, 100% utilized
        """
        spin_cpu(self.spin_secs, half_load=False)
        self.next(self.cpu_join)

    @resources(cpu=2)
    @step
    def cpu_2cores(self):
        """
        Two cores, each 100% utilized
        """
        parallel_map(lambda _: spin_cpu(self.spin_secs), [None] * 2)
        self.next(self.cpu_join)

    @resources(cpu=2)
    @step
    def cpu_1cores_halfload(self):
        """
        One core, 50% utilized
        """
        spin_cpu(self.spin_secs, half_load=True)
        self.next(self.cpu_join)

    @resources(cpu=2)
    @step
    def cpu_2cores_halfload(self):
        """
        Two cores, each 50% utilized
        """
        parallel_map(lambda _: spin_cpu(self.spin_secs, half_load=True), [None] * 2)
        self.next(self.cpu_join)

    @resources(cpu=8)
    @step
    def cpu_8cores(self):
        """
        Eight cores, each 100% utilized
        """
        parallel_map(lambda _: spin_cpu(self.spin_secs), [None] * 8)
        self.next(self.cpu_join)

    @resources(cpu=4)
    @step
    def cpu_8cores_underprovisioned(self):
        """
        Eight cores, each 100% utilized, but only 4 cores requested
        """
        parallel_map(lambda _: spin_cpu(self.spin_secs), [None] * 8)
        self.next(self.cpu_join)

    @step
    def cpu_join(self, inputs):
        self.next(self.start_mem)

    @step
    def start_mem(self):
        self.next(
            self.mem_flat_2gb,
            self.mem_flat_8gb,
            self.mem_staircase_8gb,
            self.mem_spike_2gb,
        )

    @resources(memory=3000)
    @step
    def mem_flat_2gb(self):
        """
        Allocate 2GB and sit on it
        """
        t = b"a" * 2_000_000_000
        time.sleep(self.spin_secs)
        self.next(self.mem_join)

    @resources(memory=10000)
    @step
    def mem_flat_8gb(self):
        """
        Allocate 8GB and sit on it
        """
        t = b"a" * 8_000_000_000
        time.sleep(self.spin_secs)
        self.next(self.mem_join)

    @resources(memory=10000)
    @step
    def mem_staircase_8gb(self):
        """
        Allocate 8GB in 0.5GB increments
        """
        t = []
        for i in range(16):
            t.append(b"a" * 512_000_000)
            time.sleep(self.spin_secs / 17)
        time.sleep(self.spin_secs / 17)
        self.next(self.mem_join)

    @resources(memory=3000)
    @step
    def mem_spike_2gb(self):
        """
        Spike a 2GB allocation for 10 seconds
        """
        import mmap

        time.sleep(self.spin_secs / 2)
        m = mmap.mmap(-1, 2_000_000_000, flags=mmap.MAP_PRIVATE)
        time.sleep(10)
        m.close()
        time.sleep(self.spin_secs / 2)
        self.next(self.mem_join)

    @step
    def mem_join(self, inputs):
        self.next(self.start_io)

    @step
    def start_io(self):
        self.next(self.io_write_8gb, self.io_write_8gb_mixed_cpu)

    @resources(memory=2000)
    @step
    def io_write_8gb(self):
        """
        Wait, write 8GB to a file, and wait
        """
        time.sleep(self.spin_secs / 2)
        x = b"1" * 1_000_000_000
        with open("testfile", "wb") as f:
            for i in range(8):
                f.write(x)
                f.flush()
        time.sleep(self.spin_secs / 2)
        self.next(self.io_join)

    @resources(memory=2000)
    @step
    def io_write_8gb_mixed_cpu(self):
        """
        For ten times: Write 8GB to a file, spin CPU 100%
        """
        x = b"1" * 1_000_000_000
        for _ in range(10):
            with open("testfile", "wb") as f:
                for _ in range(8):
                    f.write(x)
                    f.flush()
            spin_cpu(self.spin_secs / 10)
        self.next(self.io_join)

    @step
    def io_join(self, inputs):
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    MonitorBench()

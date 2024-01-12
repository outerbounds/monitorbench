import time, mmap, math
from tempfile import NamedTemporaryFile

from metaflow import FlowSpec, step, resources, parallel_map, Parameter, pypi_base


def load_libc():
    from ctypes.util import find_library
    from ctypes import cdll
    import ctypes

    libc = cdll.LoadLibrary(find_library("c"))
    libc.malloc.argtypes = [ctypes.c_size_t]
    libc.malloc.restype = ctypes.c_void_p
    libc.free.argtypes = [ctypes.c_void_p]
    libc.memset.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_size_t]
    libc.memset.restype = ctypes.c_void_p
    return libc


def spin_cpu(secs, half_load=False):
    nu = time.time()
    x = 0
    while time.time() - nu < secs:
        x += 1
        if half_load:
            time.sleep(0.0000002)

def spin_cpu_percentage(seconds, percentage=100):
    print(f"Running at {percentage}% CPU Utilization for {seconds} seconds...")
    for i in range(0, seconds):
        start_time = time.time()
        if i % 10 == 0:
            print(f"Beginning work cycle {i}/{seconds} seconds...")
        # Perform work for the required percentage of this second
        while (time.time() - start_time) < (percentage / 100.0):
            a = math.sqrt(64 * 64 * 64 * 64 * 64)
        # Sleep for the remainder of the second
        time.sleep(1 - percentage / 100.0)
        


def _make_file(name, size_in_mb):
    x = b"1" * 1_000_000
    with open(name, "wb") as f:
        for i in range(size_in_mb):
            f.write(x)


def _print_mem():
    import psutil

    mem = psutil.Process().memory_info()
    MB = 1024**2
    print(f"Memory (MB): Resident {mem.rss // MB} Virtual {mem.vms // MB}")


@pypi_base(python="3.11.0", packages={"psutil": "5.9.7"})
class MonitorBench(FlowSpec):
    spin_secs = Parameter(
        "step_time", help="Run each step for this many seconds", default=600
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
            self.cpu_unspecified,
            self.cpu_fraction,
            self.cpu_staircase,
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

    @resources(cpu=1)
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
    def cpu_unspecified(self):
        """
        Unspecified CPU resources.  Expected to be Metaflow default.
        Also allocates the Metaflow default amount of memory for this step.
        """
        spin_cpu(self.spin_secs, half_load=False)
        self.next(self.cpu_join)

    @resources(cpu=0.5)
    @step
    def cpu_fraction(self):
        """
        Fractional CPU resources requested.  Expected to be Metaflow default
        """
        spin_cpu(self.spin_secs, half_load=True)
        self.next(self.cpu_join)

    @resources(cpu=1)
    @step
    def cpu_staircase(self):
        """
        Increase CPU utilization in steps of 10%, ending at 100% utilization
        """
        for i in range(10):
            spin_cpu_percentage(int(self.spin_secs/10), i*10)
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
            self.mem_increasing_rss,
            self.mem_spike_2gb,
            self.mem_mmap_8gb_untouch,
            self.mem_mmap_8gb_touch_incrementally,
            self.mem_mmap_8gb_touch_all,
        )

    @resources(memory=3000)
    @step
    def mem_flat_2gb(self):
        """
        Allocate 2GB and sit on it
        """
        t = b"a" * 2_000_000_000
        _print_mem()
        time.sleep(self.spin_secs)
        self.next(self.mem_join)

    @resources(memory=10000)
    @step
    def mem_flat_8gb(self):
        """
        Allocate 8GB and sit on it
        """
        t = b"a" * 8_000_000_000
        _print_mem()
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
            _print_mem()
            time.sleep(self.spin_secs / 17)
        time.sleep(self.spin_secs / 17)
        self.next(self.mem_join)

    @resources(memory=3000)
    @step
    def mem_increasing_rss(self):
        """
        Make a 2GB allocation and fill it
        """
        m = mmap.mmap(-1, 2_000_000_000, flags=mmap.MAP_PRIVATE)
        for i in range(2_000_000_000):
            m[i] = 66
            if not i % 10_000_000:
                _print_mem()
        time.sleep(10)
        m.close()
        self.next(self.mem_join)

    @resources(memory=3000)
    @step
    def mem_spike_2gb(self):
        """
        Spike a 2GB allocation for 10secs
        """
        time.sleep(self.spin_secs / 2)
        c = load_libc()
        p = c.malloc(2_000_000_000)
        c.memset(p, 66, 2_000_000_000)
        print("spike!")
        _print_mem()
        time.sleep(10)
        c.free(p)
        print("after spike")
        _print_mem()
        time.sleep(self.spin_secs / 2)
        self.next(self.mem_join)

    @resources(memory=2000)
    @step
    def mem_mmap_8gb_untouch(self):
        """
        Mmap a 8gb file without accessing it
        """
        with NamedTemporaryFile() as tmp:
            _make_file(tmp.name, 8000)
            with open(tmp.name, "r+b") as f:
                mm = mmap.mmap(f.fileno(), 0, flags=mmap.ACCESS_READ)
                _print_mem()
                time.sleep(self.spin_secs / 2)
        self.next(self.mem_join)

    @resources(memory=10000)
    @step
    def mem_mmap_8gb_touch_incrementally(self):
        """
        Mmap a 8gb file and access it in 1GB increments
        """
        num_gigs = 8
        with NamedTemporaryFile() as tmp:
            _make_file(tmp.name, num_gigs * 1000)
            with open(tmp.name, "r+b") as f:
                r = 0
                B = int(1e9)
                mm = mmap.mmap(f.fileno(), 0, flags=mmap.ACCESS_READ)
                for i in range(num_gigs):
                    b = str(mm[i * B : (i + 1) * B])
                    _print_mem()
                    time.sleep(1)
        self.next(self.mem_join)

    @resources(memory=32000)
    @step
    def mem_mmap_8gb_touch_all(self):
        """
        Mmap a 8gb file and access it at once
        """
        num_gigs = 8
        with NamedTemporaryFile() as tmp:
            _make_file(tmp.name, num_gigs * 1000)
            with open(tmp.name, "r+b") as f:
                mm = mmap.mmap(f.fileno(), 0, flags=mmap.ACCESS_READ)
                x = str(mm[:])
                _print_mem()
                time.sleep(self.spin_secs / 2)
        x[0]
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
        num_gigs = 8
        with NamedTemporaryFile() as tmp:
            _make_file(tmp.name, num_gigs * 1000)
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
            num_gigs = 8
            with NamedTemporaryFile() as tmp:
                _make_file(tmp.name, num_gigs * 1000)
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

import numpy
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


POINTER_SIZE = 8
UNSIGNED_INT_SIZE = 8
BITS_PER_BYTE = 8

PLOT_FONT_SIZE = 14

plt.rcParams.update(
    {
        "font.size": PLOT_FONT_SIZE,
        "axes.labelsize": PLOT_FONT_SIZE,
        "xtick.labelsize": PLOT_FONT_SIZE,
        "ytick.labelsize": PLOT_FONT_SIZE,
    }
)


# Formulas
def calculate_key_storage_overhead(key_size, num_keys):
    return (key_size + 16) * num_keys


def empty_keyset(amount):
    return numpy.zeros(amount)


def vector(key_size, num_keys):
    key_overhead = calculate_key_storage_overhead(key_size, num_keys)
    data_structure_overhead = 32
    return key_overhead + (POINTER_SIZE * num_keys) + data_structure_overhead


def vector_with_optional_values_and_hashset(key_size, num_keys):
    key_overhead = calculate_key_storage_overhead(key_size, num_keys)
    data_structure_overhead = 88
    load_factor = 0.875
    return (
        key_overhead
        + (POINTER_SIZE * num_keys)
        + numpy.ceil((POINTER_SIZE * num_keys) / load_factor)
        + data_structure_overhead
    )


def vector_with_hashset(key_size, num_keys):
    key_overhead = calculate_key_storage_overhead(key_size, num_keys)
    data_structure_overhead = 80
    load_factor = 0.875
    return (
        key_overhead
        + (POINTER_SIZE * num_keys)
        + numpy.ceil((POINTER_SIZE * num_keys) / load_factor)
        + data_structure_overhead
    )


def bloom_filter(_key_size, num_keys, shape):
    data_structure_overhead = 80
    false_positive_rate = 0.01
    return numpy.full(
        shape,
        numpy.ceil(
            numpy.ceil(
                num_keys * (numpy.log(1 / false_positive_rate) / (numpy.log(2) ** 2))
            )
            / BITS_PER_BYTE
        )
        + data_structure_overhead,
    )


def vector_with_bloom_filter(key_size, num_keys):
    key_overhead = calculate_key_storage_overhead(key_size, num_keys)
    data_structure_overhead = 104
    false_positive_rate = 0.01
    return (
        key_overhead
        + (POINTER_SIZE * num_keys)
        + numpy.ceil(
            (
                numpy.ceil(
                    num_keys
                    * (numpy.log(1 / false_positive_rate) / (numpy.log(2) ** 2))
                )
            )
            / BITS_PER_BYTE
        )
        + data_structure_overhead
    )


def vector_with_hashmap(key_size, num_keys):
    key_overhead = calculate_key_storage_overhead(key_size, num_keys)
    data_structure_overhead = 80
    load_factor = 0.875
    return (
        key_overhead
        + (POINTER_SIZE * num_keys)
        + numpy.ceil(((POINTER_SIZE + UNSIGNED_INT_SIZE) * num_keys) / load_factor)
        + data_structure_overhead
    )


def cuckoo_filter(key_size, num_keys, shape):
    fingerprint_size_bytes = 1
    # items_per_bucket = 4
    load_factor = 0.95
    return numpy.full(
        shape, numpy.ceil((fingerprint_size_bytes * num_keys) / load_factor)
    )


def b_plus_tree(key_size, num_keys):
    key_overhead = calculate_key_storage_overhead(key_size, num_keys)
    node_size_bytes = 128
    fanout = 128 / (POINTER_SIZE * 2)
    return key_overhead + numpy.ceil(
        ((numpy.ceil(num_keys / (fanout - 1)) * node_size_bytes) / 0.67)
    )


if __name__ == "__main__":
    key_size = 64
    num_keys = numpy.arange(1, 1000000)
    plt.figure(figsize=(12, 6))

    plt.plot(num_keys, empty_keyset(len(num_keys)), label="Empty Keyset")
    plt.plot(num_keys, vector(key_size, num_keys), label="Vector")
    plt.plot(
        num_keys,
        vector_with_optional_values_and_hashset(key_size, num_keys),
        label="Vector + Optional + Hashset",
    )
    plt.plot(
        num_keys, vector_with_hashset(key_size, num_keys), label="Vector + Hashset"
    )
    plt.plot(
        num_keys, bloom_filter(key_size, num_keys, len(num_keys)), label="Bloom Filter"
    )
    plt.plot(
        num_keys,
        vector_with_bloom_filter(key_size, num_keys),
        label="Vector + Bloom Filter",
    )
    plt.plot(
        num_keys, vector_with_hashmap(key_size, num_keys), label="Vector + Hashmap"
    )
    plt.plot(
        num_keys,
        cuckoo_filter(key_size, num_keys, len(num_keys)),
        label="Cuckoo Filter",
    )
    plt.plot(num_keys, b_plus_tree(key_size, num_keys), label="B+ Tree")

    plt.xlabel("Number of Keys")
    plt.ylabel("Memory (bytes)")
    plt.title(f"Data Structure Overhead (Key Size={key_size})")
    plt.legend()
    plt.tight_layout()
    plt.savefig("fixed_key_size.pdf")

    num_keys = 100000
    key_size = numpy.arange(1, 100000)
    plt.figure(figsize=(12, 6))

    plt.plot(key_size, empty_keyset(len(key_size)), label="Empty Keyset")
    plt.plot(key_size, vector(key_size, num_keys), label="Vector")
    plt.plot(
        key_size,
        vector_with_optional_values_and_hashset(key_size, num_keys),
        label="Vector + Optional + Hashset",
    )
    plt.plot(
        key_size, vector_with_hashset(key_size, num_keys), label="Vector + Hashset"
    )
    plt.plot(
        key_size, bloom_filter(key_size, num_keys, len(key_size)), label="Bloom Filter"
    )
    plt.plot(
        key_size,
        vector_with_bloom_filter(key_size, num_keys),
        label="Vector + Bloom Filter",
    )
    plt.plot(
        key_size, vector_with_hashmap(key_size, num_keys), label="Vector + Hashmap"
    )
    plt.plot(
        key_size,
        cuckoo_filter(key_size, num_keys, len(key_size)),
        label="Cuckoo Filter",
    )
    plt.plot(key_size, b_plus_tree(key_size, num_keys), label="B+ Tree")

    plt.xlabel("Key Size (bytes)")
    plt.ylabel("Memory (bytes)")
    plt.title(f"Data Structure Overhead (Number of Keys={100000})")
    plt.legend()
    plt.tight_layout()
    plt.savefig("fixed_number_of_keys.pdf")

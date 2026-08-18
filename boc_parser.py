"""
Minimal pure-Python TON BOC (Bag of Cells) deserializer.
No external dependencies — written from the documented BOC/Cell binary format
since no TON library is installable in this sandbox (no network access).
"""
import base64


class BitReader:
    def __init__(self, data: bytes, num_bits: int):
        self.data = data
        self.num_bits = num_bits
        self.pos = 0

    def remaining(self):
        return self.num_bits - self.pos

    def read_bit(self):
        byte_idx = self.pos // 8
        bit_idx = 7 - (self.pos % 8)
        bit = (self.data[byte_idx] >> bit_idx) & 1
        self.pos += 1
        return bit

    def read_uint(self, n):
        v = 0
        for _ in range(n):
            v = (v << 1) | self.read_bit()
        return v

    def read_int(self, n):
        v = self.read_uint(n)
        if v >= (1 << (n - 1)):
            v -= (1 << n)
        return v

    def read_bytes(self, n):
        # Read n bytes bit-by-bit; works regardless of current alignment.
        out = bytearray()
        for _ in range(n):
            byte = 0
            for _ in range(8):
                byte = (byte << 1) | self.read_bit()
            out.append(byte)
        return bytes(out)

    def read_coins(self):
        # VarUInteger 16: first 4 bits = length in bytes, then that many bytes big-endian
        length = self.read_uint(4)
        if length == 0:
            return 0
        val = 0
        for _ in range(length):
            val = (val << 8) | self.read_uint(8)
        return val

    def read_address(self):
        # MsgAddress (addr_std, no anycast) — most common case
        tag = self.read_uint(2)
        if tag == 0:
            return None  # addr_none
        if tag == 2:  # addr_std
            anycast = self.read_bit()
            if anycast:
                depth = self.read_uint(5)
                self.read_uint(depth)  # rewrite_pfx, skip
            workchain = self.read_int(8)
            addr_hash = self.read_bytes(32)
            return f"{workchain}:{addr_hash.hex()}"
        return f"<unsupported addr tag {tag}>"


class Cell:
    def __init__(self, bits: bytes, num_bits: int, refs: list, is_exotic: bool = False):
        self.bits = bits
        self.num_bits = num_bits
        self.refs = refs
        self.is_exotic = is_exotic

    def reader(self):
        return BitReader(self.bits, self.num_bits)


def parse_boc(b64_or_bytes) -> Cell:
    if isinstance(b64_or_bytes, str):
        data = base64.b64decode(b64_or_bytes)
    else:
        data = b64_or_bytes

    assert data[0:4] == bytes.fromhex("b5ee9c72"), "not a standard BOC"

    flags_byte = data[4]
    has_idx = (flags_byte >> 7) & 1
    has_crc32c = (flags_byte >> 6) & 1
    has_cache_bits = (flags_byte >> 5) & 1
    size_bytes = flags_byte & 0b111

    off_bytes = data[5]

    p = 6

    def read_n(nbytes):
        nonlocal p
        v = int.from_bytes(data[p:p + nbytes], "big")
        p += nbytes
        return v

    cells_count = read_n(size_bytes)
    roots_count = read_n(size_bytes)
    absent_count = read_n(size_bytes)
    tot_cells_size = read_n(off_bytes)

    root_list = [read_n(size_bytes) for _ in range(roots_count)]

    if has_idx:
        p += cells_count * off_bytes  # skip index, we don't need it

    cell_data_start = p
    cell_data = data[p:p + tot_cells_size]
    p += tot_cells_size

    # Parse each cell's raw descriptor + data + ref indices
    raw_cells = []
    cp = 0
    for i in range(cells_count):
        d1 = cell_data[cp]
        d2 = cell_data[cp + 1]
        cp += 2
        refs_count = d1 & 0b111
        is_exotic = bool(d1 & 0b1000)
        data_len_bytes = (d2 >> 1) + (d2 & 1)
        full_byte = (d2 & 1) == 0
        raw_bits_data = cell_data[cp:cp + data_len_bytes]
        cp += data_len_bytes
        if full_byte:
            num_bits = data_len_bytes * 8
        else:
            # last byte encodes bit length via trailing '1' marker bit (standard TON rule)
            last = raw_bits_data[-1]
            num_bits = (data_len_bytes - 1) * 8
            # find highest set bit position from the LSB side (marker bit)
            for b in range(8):
                if (last >> b) & 1:
                    num_bits += (7 - b)
                    break
        ref_indices = []
        for _ in range(refs_count):
            ref_indices.append(read_n_from(cell_data, cp, size_bytes))
            cp += size_bytes
        raw_cells.append({
            "bits": raw_bits_data,
            "num_bits": num_bits,
            "ref_indices": ref_indices,
            "is_exotic": is_exotic,
        })

    # Build actual Cell objects, resolving refs (cells reference later or earlier indices; standard BOC lists children after parents in most serializers, but resolve generically)
    built = {}

    def build(idx):
        if idx in built:
            return built[idx]
        raw = raw_cells[idx]
        refs = [build(r) for r in raw["ref_indices"]]
        c = Cell(raw["bits"], raw["num_bits"], refs, raw["is_exotic"])
        built[idx] = c
        return c

    for i in range(cells_count):
        build(i)

    return built[root_list[0]]


def read_n_from(data, pos, nbytes):
    return int.from_bytes(data[pos:pos + nbytes], "big")


class BitWriter:
    """Builds a single TON cell's bit content (no refs needed for our swap body)."""
    def __init__(self):
        self.bits = []  # list of 0/1 ints

    def write_uint(self, value, n):
        for i in range(n - 1, -1, -1):
            self.bits.append((value >> i) & 1)

    def write_coins(self, value: int):
        # VarUInteger16: 4-bit length (in bytes), then that many bytes big-endian
        if value == 0:
            self.write_uint(0, 4)
            return
        nbytes = (value.bit_length() + 7) // 8
        self.write_uint(nbytes, 4)
        self.write_uint(value, nbytes * 8)

    def to_cell_bytes(self):
        """Returns (data_bytes, num_bits) with TON's standard bit-padding marker
        applied when the bit count isn't a multiple of 8, matching the format
        the decoder above expects (and what real TON cells use)."""
        num_bits = len(self.bits)
        if num_bits % 8 == 0:
            data = bytearray()
            for i in range(0, num_bits, 8):
                byte = 0
                for b in self.bits[i:i + 8]:
                    byte = (byte << 1) | b
                data.append(byte)
            return bytes(data), num_bits
        else:
            padded_bits = list(self.bits)
            padded_bits.append(1)  # completion marker
            while len(padded_bits) % 8 != 0:
                padded_bits.append(0)
            data = bytearray()
            for i in range(0, len(padded_bits), 8):
                byte = 0
                for b in padded_bits[i:i + 8]:
                    byte = (byte << 1) | b
                data.append(byte)
            return bytes(data), num_bits


def build_single_cell_boc(bit_writer: BitWriter) -> bytes:
    """Serializes a single cell with zero refs into a standard BOC (no CRC32C —
    that field is optional per spec; omitting it avoids a subtle checksum bug
    since we can't test against a live client from this sandbox)."""
    data, num_bits = bit_writer.to_cell_bytes()
    full_byte = (num_bits % 8 == 0)
    d2 = (len(data) * 2) - (0 if full_byte else 1)
    d1 = 0  # 0 refs, not exotic, level 0

    cell_bytes = bytes([d1, d2]) + data
    tot_cells_size = len(cell_bytes)

    size_bytes = 1     # 1 cell total — fits in 1 byte
    off_bytes = 1 if tot_cells_size < 256 else 2

    header = bytes.fromhex("b5ee9c72")
    flags_byte = bytes([(0 << 7) | (0 << 6) | (0 << 5) | size_bytes])  # no idx, no crc32c
    off_bytes_field = bytes([off_bytes])

    def enc(v, n):
        return v.to_bytes(n, "big")

    cells_count = enc(1, size_bytes)
    roots_count = enc(1, size_bytes)
    absent_count = enc(0, size_bytes)
    tot_size_field = enc(tot_cells_size, off_bytes)
    root_list = enc(0, size_bytes)

    boc = header + flags_byte + off_bytes_field + cells_count + roots_count + absent_count + tot_size_field + root_list + cell_bytes
    return boc


def build_swap_v2_payload(query_id: int, amount_nano: int) -> str:
    """Builds the verified swap#a5a7cbf8 message body for DeDust CPMM v2 pools:
    swap#a5a7cbf8 query_id:uint64 amount:Coins = InternalMsgBody;
    Decoded directly from a real, successful on-chain swap transaction — not
    guessed from documentation. Returns a base64-encoded BOC ready to hand to
    TonConnect as the message payload."""
    import base64
    w = BitWriter()
    w.write_uint(0xa5a7cbf8, 32)
    w.write_uint(query_id, 64)
    w.write_coins(amount_nano)
    boc = build_single_cell_boc(w)
    return base64.b64encode(boc).decode("ascii")

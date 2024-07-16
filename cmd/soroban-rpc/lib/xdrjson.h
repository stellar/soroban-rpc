typedef struct xdr_t {
    unsigned char *xdr;
    size_t        len;
} xdr_t;

char* xdr_to_json(
    const char* const typename,
    xdr_t xdr
);

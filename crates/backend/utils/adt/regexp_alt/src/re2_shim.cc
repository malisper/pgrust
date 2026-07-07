// Minimal C ABI over RE2 (the product alternate regexp engine). Flags ride
// as inline (?i)(?s)(?m) groups so the shim works across RE2 option-set
// vintages; longest selects POSIX leftmost-longest matching, the mode the
// auto-dispatch compatibility class is proven against.
#include <re2/re2.h>

#include <cstring>
#include <string>
#include <vector>

extern "C" {

void* pgr_re2_compile(const char* pat, int len, int literal, int longest, char* errbuf,
                      int errbuf_len) {
    RE2::Options opts;
    opts.set_log_errors(false);
    opts.set_literal(literal != 0);
    opts.set_longest_match(longest != 0);
    opts.set_max_mem(64 << 20);
    RE2* re = new RE2(re2::StringPiece(pat, len), opts);
    if (!re->ok()) {
        std::string err = re->error();
        int n = (int)err.size();
        if (n >= errbuf_len) n = errbuf_len - 1;
        std::memcpy(errbuf, err.data(), n);
        errbuf[n] = '\0';
        delete re;
        return nullptr;
    }
    return re;
}

void pgr_re2_free(void* rev) {
    delete static_cast<RE2*>(rev);
}

int pgr_re2_ngroups(void* rev) {
    return static_cast<RE2*>(rev)->NumberOfCapturingGroups();
}

// groups receives ngroups (start,end) byte-offset pairs, -1/-1 for a group
// that did not participate. Returns 1 on match, 0 on no match.
int pgr_re2_match(void* rev, const char* text, int len, int startpos, int ngroups, long long* groups) {
    RE2* re = static_cast<RE2*>(rev);
    std::vector<re2::StringPiece> m(ngroups);
    re2::StringPiece sp(text, len);
    if (!re->Match(sp, startpos, len, RE2::UNANCHORED, m.data(), ngroups)) {
        return 0;
    }
    for (int i = 0; i < ngroups; i++) {
        if (m[i].data() == nullptr) {
            groups[2 * i] = -1;
            groups[2 * i + 1] = -1;
        } else {
            groups[2 * i] = (long long)(m[i].data() - text);
            groups[2 * i + 1] = groups[2 * i] + (long long)m[i].size();
        }
    }
    return 1;
}

}  // extern "C"

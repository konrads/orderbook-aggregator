export COVERAGE_THRESHOLD=60

.PHONY: setup-coverage test-with-coverage markdown-coverage-report html-coverage-report check-markdown-coverage-threshold

validate-coverage: setup-coverage test-with-coverage markdown-coverage-report check-markdown-coverage-threshold

setup-coverage:
	rm -rf coverage
	rustup component add llvm-tools-preview
	cargo install grcov
	@mkdir -p coverage

test-with-coverage:
	CARGO_INCREMENTAL=0 RUSTFLAGS=-Cinstrument-coverage LLVM_PROFILE_FILE=coverage/raw/cargo-test-%p-%m.profraw cargo test

markdown-coverage-report: 
	@grcov . --binary-path ./target/debug/deps -s . -t markdown --branch --ignore-not-existing --ignore "src/tests" --ignore "src/test*.rs" --ignore "*/.cargo/*" --ignore "target/*" -o coverage/REPORT.md

html-coverage-report:
	@grcov . --binary-path ./target/debug/deps -s . -t html --branch --ignore-not-existing --ignore "src/tests" --ignore "src/test*.rs" --ignore "*/.cargo/*" --ignore "target/*" -o coverage/html

check-markdown-coverage-threshold:
	@awk -F ': ' -v threshold="$(COVERAGE_THRESHOLD)" '/Total coverage:/ { gsub("%", "", $$NF); if ($$NF < threshold) { print "Insufficient code coverage:", $$NF, "<", threshold; exit 1 } else print "Sufficient code coverage:", $$NF, ">=", threshold }' coverage/REPORT.md
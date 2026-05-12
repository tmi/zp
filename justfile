clean:
    @for dir in cs*; do \
        echo "considering $dir" ; \
        if [ -f "$dir/justfile" ]; then \
            echo "Cleaning $dir..."; \
            just -f "$dir/justfile" -d "$dir" clean; \
        fi; \
    done

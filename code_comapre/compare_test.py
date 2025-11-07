import difflib
import html

def show_diff(version1: str, version2: str):
    """
    Console-based diff display (original function for CLI usage)
    """
    old_lines = version1.strip().splitlines()
    new_lines = version2.strip().splitlines()

    diff = difflib.unified_diff(
        old_lines,
        new_lines,
        fromfile='Version 1',
        tofile='Version 2',
        lineterm='',
        n=0  # show only changed lines (no extra context)
    )

    old_line = 0
    new_line = 0

    for line in diff:
        if line.startswith('@@'):
            # Extract starting line numbers from hunk header (e.g., @@ -16 +16 @@)
            parts = line.split()
            old_line = int(parts[1].split(',')[0][1:])  # e.g., -16 -> 16
            new_line = int(parts[2].split(',')[0][1:])  # e.g., +16 -> 16
            continue

        elif line.startswith('---') or line.startswith('+++'):
            continue

        elif line.startswith('-'):
            print(f"\033[91m- [Old:{old_line:>3}] {line[1:]}\033[0m")
            old_line += 1

        elif line.startswith('+'):
            print(f"\033[92m+ [New:{new_line:>3}] {line[1:]}\033[0m")
            new_line += 1


def compare_for_ui(version1: str, version2: str):
    """
    Generate HTML-formatted side-by-side diff for UI display with full code
    Returns: dict with diff_html, additions, deletions, changes, has_changes
    """
    if not version1.strip() or not version2.strip():
        return {
            'diff_html': '',
            'additions': 0,
            'deletions': 0,
            'changes': 0,
            'has_changes': False
        }
    
    old_lines = version1.strip().splitlines()
    new_lines = version2.strip().splitlines()

    # Use SequenceMatcher for better diff with full context
    matcher = difflib.SequenceMatcher(None, old_lines, new_lines)
    
    # Generate side-by-side HTML with full code
    left_html = []
    right_html = []
    
    additions = 0
    deletions = 0
    
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == 'equal':
            # Lines are the same in both versions
            for i, j in zip(range(i1, i2), range(j1, j2)):
                escaped_line = html.escape(old_lines[i])
                # Left side
                left_html.append(
                    f'<div class="diff-row context">'
                    f'<div class="line-number">{i + 1}</div>'
                    f'<div class="line-content">{escaped_line}</div>'
                    f'</div>'
                )
                # Right side (same content)
                right_html.append(
                    f'<div class="diff-row context">'
                    f'<div class="line-number">{j + 1}</div>'
                    f'<div class="line-content">{escaped_line}</div>'
                    f'</div>'
                )
        
        elif tag == 'delete':
            # Lines only in version 1 (deleted)
            deletions += (i2 - i1)
            for i in range(i1, i2):
                escaped_line = html.escape(old_lines[i])
                # Left side - show deletion
                left_html.append(
                    f'<div class="diff-row deletion">'
                    f'<div class="line-number">{i + 1}</div>'
                    f'<div class="line-content">{escaped_line}</div>'
                    f'</div>'
                )
                # Right side - empty
                right_html.append(
                    f'<div class="diff-row empty">'
                    f'<div class="line-number"></div>'
                    f'<div class="line-content"></div>'
                    f'</div>'
                )
        
        elif tag == 'insert':
            # Lines only in version 2 (added)
            additions += (j2 - j1)
            for j in range(j1, j2):
                escaped_line = html.escape(new_lines[j])
                # Left side - empty
                left_html.append(
                    f'<div class="diff-row empty">'
                    f'<div class="line-number"></div>'
                    f'<div class="line-content"></div>'
                    f'</div>'
                )
                # Right side - show addition
                right_html.append(
                    f'<div class="diff-row addition">'
                    f'<div class="line-number">{j + 1}</div>'
                    f'<div class="line-content">{escaped_line}</div>'
                    f'</div>'
                )
        
        elif tag == 'replace':
            # Lines changed between versions
            deletions += (i2 - i1)
            additions += (j2 - j1)
            
            # Show all old lines as deletions
            for i in range(i1, i2):
                escaped_line = html.escape(old_lines[i])
                left_html.append(
                    f'<div class="diff-row deletion">'
                    f'<div class="line-number">{i + 1}</div>'
                    f'<div class="line-content">{escaped_line}</div>'
                    f'</div>'
                )
            
            # Show all new lines as additions
            for j in range(j1, j2):
                escaped_line = html.escape(new_lines[j])
                right_html.append(
                    f'<div class="diff-row addition">'
                    f'<div class="line-number">{j + 1}</div>'
                    f'<div class="line-content">{escaped_line}</div>'
                    f'</div>'
                )
            
            # Pad the shorter side with empty rows for alignment
            len_old = i2 - i1
            len_new = j2 - j1
            if len_old < len_new:
                for _ in range(len_new - len_old):
                    left_html.append(
                        f'<div class="diff-row empty">'
                        f'<div class="line-number"></div>'
                        f'<div class="line-content"></div>'
                        f'</div>'
                    )
            elif len_new < len_old:
                for _ in range(len_old - len_new):
                    right_html.append(
                        f'<div class="diff-row empty">'
                        f'<div class="line-number"></div>'
                        f'<div class="line-content"></div>'
                        f'</div>'
                    )
    
    changes = additions + deletions
    has_changes = changes > 0
    
    # Combine left and right panels
    diff_html = (
        f'<div class="diff-left">{"".join(left_html)}</div>'
        f'<div class="diff-right">{"".join(right_html)}</div>'
    )
    
    return {
        'diff_html': diff_html,
        'additions': additions,
        'deletions': deletions,
        'changes': changes,
        'has_changes': has_changes
    }

def main():
    version1 = r"""import os
    from sqlalchemy import create_engine

    # Update this path to match your local project structure
    COMMON_PATH = r"c:\Users\sukhd\Projects\HE_PDM_code\hrp.pdm.schema.service\src\main\resources\db\changelog\configdb\changelogs"

    # Validate that the base path exists - if not, provide helpful error message
    if not os.path.exists(COMMON_PATH):
        print(f"WARNING: COMMON_PATH does not exist: {COMMON_PATH}")
    HF_PATH = os.path.join(COMMON_PATH, "tenants", "healthfirst")
    PEHP_PATH = os.path.join(COMMON_PATH, "tenants", "pehp")
    SUTTER_PATH = os.path.join(COMMON_PATH, "tenants", "sutter")
    HSYNC_PATH = os.path.join(COMMON_PATH, "tenants", "healthsync")

    # DB ENVIRONMENT VARIABLES
    USER = "postgres"
    PASSWORD = "postgres"
    HOST = "localhost"
    PORT = "5434"
    DB = "postgres"

    ENGINE = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}")

    #comparison test
    TENANT_DEV_FILE_PATHS = {
        "common": COMMON_PATH,
        "healthfirst": HF_PATH,
        "pehp": PEHP_PATH,
        "sutter": SUTTER_PATH,
        "healthsync": HSYNC_PATH,
    }

    TENANT_DATA_FOLDER_PATHS = {
        k: os.path.join(v, "data") for k, v in TENANT_DEV_FILE_PATHS.items()
    }

    # Note: Sheet names are now auto-detected based on Rule IDs in the master file
    # No need for manual SHEET_NAME mapping anymore"""

    version2 = r"""import os
    from sqlalchemy import create_engine

    # Update this path to match your local project structure
    COMMON_PATH = r"c:\Users\sukhd\Projects\HE_PDM_code\hrp.pdm.schema.service\src\main\resources\db\changelog\configdb\changelogs"

    # Validate that the base path exists - if not, provide helpful error message
    if not os.path.exists(COMMON_PATH):
        print(f"WARNING: COMMON_PATH does not exist: {COMMON_PATH}")
    HF_PATH = os.path.join(COMMON_PATH, "tenants", "healthfirst")
    PEHP_PATH = os.path.join(COMMON_PATH, "tenants", "pehp")
    SUTTER_PATH = os.path.join(COMMON_PATH, "tenants", "sutter")
    HSYNC_PATH = os.path.join(COMMON_PATH, "tenants", "healthsync")

    # DB ENVIRONMENT VARIABLES
    USER = "postgres2"
    PASSWORD = "postgres"
    HOST = "localhost"
    PORT = "5434"
    DB = "postgres"

    ENGINE = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}")


    TENANT_FILE_PATHS = {
        "common": COMMON_PATH,
        "healthfirst": HF_PATH,
        "pehp": PEHP_PATH,
        "sutter": SUTTER_PATH,
        "healthsync": HSYNC_PATH,
    }

    TENANT_DATA_FOLDER_PATHS = {
        k: os.path.join(v, "data") for k, v in TENANT_DEV_FILE_PATHS.items()
    }

    # Note: Sheet names are now auto-detected based on Rule IDs in the master file
    # No need for manual SHEET_NAME mapping anymore"""

    print("Comparing two versions of the code:\n")
    show_diff(version1, version2)
    print("\nComparison complete.")

if __name__ == "__main__":
    main()
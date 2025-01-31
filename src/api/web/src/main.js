const webdav_server = (import.meta.env.PROD)
    ? window.location.href
    : 'http://127.0.0.1:8080/';

const file_svg = `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-file-earmark" viewBox="0 0 16 16">
  <path d="M14 4.5V14a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V2a2 2 0 0 1 2-2h5.5zm-3 0A1.5 1.5 0 0 1 9.5 3V1H4a1 1 0 0 0-1 1v12a1 1 0 0 0 1 1h8a1 1 0 0 0 1-1V4.5z"/>
</svg>`;

const dir_svg = `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-folder" viewBox="0 0 16 16">
  <path d="M.54 3.87.5 3a2 2 0 0 1 2-2h3.672a2 2 0 0 1 1.414.586l.828.828A2 2 0 0 0 9.828 3h3.982a2 2 0 0 1 1.992 2.181l-.637 7A2 2 0 0 1 13.174 14H2.826a2 2 0 0 1-1.991-1.819l-.637-7a2 2 0 0 1 .342-1.31zM2.19 4a1 1 0 0 0-.996 1.09l.637 7a1 1 0 0 0 .995.91h10.348a1 1 0 0 0 .995-.91l.637-7A1 1 0 0 0 13.81 4zm4.69-1.707A1 1 0 0 0 6.172 2H2.5a1 1 0 0 0-1 .981l.006.139q.323-.119.684-.12h5.396z"/>
</svg>`;

const return_svg = `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-arrow-return-left" viewBox="0 0 16 16">
  <path fill-rule="evenodd" d="M14.5 1.5a.5.5 0 0 1 .5.5v4.8a2.5 2.5 0 0 1-2.5 2.5H2.707l3.347 3.346a.5.5 0 0 1-.708.708l-4.2-4.2a.5.5 0 0 1 0-.708l4-4a.5.5 0 1 1 .708.708L2.707 8.3H12.5A1.5 1.5 0 0 0 14 6.8V2a.5.5 0 0 1 .5-.5"/>
</svg>`;

async function list_directory(path) {
    const path_without_slash = path.endsWith('/') ? path.slice(0, -1) : path;
    const result = await fetch(webdav_server + path_without_slash, {
        method: 'PROPFIND',
        headers: {
            'Depth': '1',
        },
        body: '<?xml version="1.0" encoding="utf-8" ?><D:propfind xmlns:D="DAV:"><D:allprop/></D:propfind>'
    });
    const parser = new DOMParser();
    const xmlDoc = parser.parseFromString(await result.text(), "text/xml");
    const entries = [];

    for (const resp of xmlDoc.getElementsByTagName('D:response')) {
        let prop = resp.getElementsByTagName('D:prop')[0];

        const info = {
            href: resp.childNodes[0].textContent,
            name: prop.getElementsByTagName('D:displayname')[0].textContent,
            kind: prop.getElementsByTagName('D:resourcetype')[0].childNodes.length > 0 ? 'directory' : 'file',
            size: prop.getElementsByTagName('D:getcontentlength')[0].textContent,
            etag: prop.getElementsByTagName('D:getetag')[0].textContent,
            content_type: prop.getElementsByTagName('D:getcontenttype')[0]?.textContent ?? 'application/octet-stream',
            creation_date: prop.getElementsByTagName('D:creationdate')[0].textContent,
            last_modified: prop.getElementsByTagName('D:getlastmodified')[0].textContent,
        };

        if (info.href === `/${path}/`) {
            info.name = '.';
        }

        if (info.name === '.' || info.name === '..' || info.href === '/') {
            continue;
        }

        entries.push(info);
    }

    entries.sort((a, b) => {
        let kind = a.kind.localeCompare(b.kind);
        if (kind !== 0) {
            return kind;
        }

        return a.name.localeCompare(b.name);
    })

    return entries;
}

async function render_files(entries) {
    const file_list = document.querySelector('.file-list');
    file_list.style.display = 'block';
    const table = file_list.querySelector('tbody');

    // Clear table
    table.innerHTML = '';

    // Add content
    for (let entry of entries) {
        const tr = document.createElement('tr');
        const td_name = document.createElement('td');
        const td_size = document.createElement('td');
        const td_etag = document.createElement('td');
        const td_modified = document.createElement('td');

        const name_link = document.createElement('a');
        name_link.classList.add('file-link');
        if (entry.kind === 'directory') {
            name_link.innerHTML = entry.name === '..' ? return_svg : dir_svg;
            name_link.append(document.createTextNode(entry.name + '/'));
            name_link.setAttribute('href', '#?path=' + encodeURIComponent(entry.href));
        } else {
            name_link.innerHTML = file_svg;
            name_link.append(document.createTextNode(entry.name));
            name_link.setAttribute('href', (new URL(entry.href, webdav_server)).toString());
            name_link.setAttribute('target', '_blank');
        }
        name_link.onclick = (e) => {
            if (entry.kind === 'directory') {
                e.preventDefault();
                selectPath(entry.href).then(undefined);
            }
        };
        td_name.appendChild(name_link);
        td_size.textContent = format_size(entry.size);
        td_etag.textContent = entry.etag.slice(0, 8);
        td_modified.textContent = entry.last_modified;

        tr.appendChild(td_name);
        tr.appendChild(td_size);
        tr.appendChild(td_etag);
        tr.appendChild(td_modified);
        table.appendChild(tr);
    }
}

async function update_path_display(path) {
    const current_path = document.querySelector('.current-path');
    current_path.innerHTML = '';
    const segments = path.split('/').filter((segment) => segment !== '');
    let subpath = '';

    for (let segment_name of segments) {
        subpath = subpath + '/' + segment_name;
        const segment_path = subpath === '' ? '/' : subpath;

        const path_segment = document.createElement('a');
        path_segment.classList.add('path-segment');
        path_segment.setAttribute('href', '#?path=' + encodeURIComponent(segment_path));
        path_segment.textContent = segment_name;
        path_segment.onclick = (e) => {
            e.preventDefault();
            selectPath(segment_path).then(undefined);
        };
        current_path.appendChild(path_segment);
    }
}

async function selectPath(path) {
    // Remove trailing slash
    path = path.startsWith('/') ? path.slice(1) : path;
    path = (path === '' || path === '..' || path === '/..') ? '/' : path;

    let validSegments = [];
    for (let segment of path.split('/')) {
        if (segment === '..') {
            if (validSegments.length) validSegments.pop();
        } else if (segment !== '.') {
            validSegments.push(segment);
        }
    }

    path = validSegments.length
        ? validSegments.join('/')
        : '/';

    let entries = await list_directory(path);
    await render_files(entries);
    await update_path_display(path);
}

function format_size(size) {
    if (size === '0') {
        return '-';
    }
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let unit = 0;
    let size_number = parseInt(size);

    while (size_number > 1024) {
        size_number /= 1024;
        unit++;
    }

    return size_number.toFixed(2) + ' ' + units[unit];
}

async function load() {
    await selectPath('/');
    document.querySelector('.loading').remove();
}

load().then(undefined);

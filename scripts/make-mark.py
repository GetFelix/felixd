"""Draws docs/brand/felixd-mark.png from the main Felix logo.

    python3 scripts/make-mark.py <felix>/docs/assets/logo.PNG docs/brand/felixd-mark.png

Needs Pillow.
"""
import math, sys
from PIL import Image, ImageChops, ImageDraw, ImageFilter

S = 2                      # supersample
W, H = 1000 * S, 1000 * S
CX, CY, R = 500 * S, 440 * S, 400 * S
CYAN = (11, 165, 200, 255)      # hsl(192, 90%, 46%)
CYAN_L = (120, 225, 245, 255)
INK = (16, 18, 24, 255)
AMBER = (251, 191, 36, 255)
WHITE = (255, 255, 255, 255)

out = Image.new("RGBA", (W, H), (0, 0, 0, 0))

# Cat head from the main logo, outer ring erased, clipped to the disk.
logo = Image.open(sys.argv[1]).convert("RGBA")
head = logo.crop((140, 100, 360, 290))
bw = head.convert("L").point(lambda v: 0 if v < 128 else 255)
# The crop catches part of the logo's own ring; flood it away from the edges.
for x in range(bw.width):
    if bw.getpixel((x, 0)) == 0:
        ImageDraw.floodfill(bw, (x, 0), 255)
for y in range(0, 120):
    if bw.getpixel((0, y)) == 0:
        ImageDraw.floodfill(bw, (0, y), 255)
# Whiten what the flood removed; everything else keeps its grey edges.
gray = head.convert("L")
flooded = ImageChops.subtract(bw, gray.point(lambda v: 0 if v < 128 else 255))
gray = ImageChops.lighter(gray, flooded)
scale = 3.15 * S
gray = gray.resize((int(gray.width * scale), int(gray.height * scale)), Image.BICUBIC).filter(ImageFilter.GaussianBlur(1.2 * S))
# Steep curve: crisp black and white with a two pixel anti-aliased edge.
g = gray.point(lambda v: max(0, min(255, int((v - 128) * 6 + 128))))
head = Image.merge("RGBA", (g, g, g, Image.new("L", g.size, 255)))
disk = Image.new("L", (W, H), 0)
ImageDraw.Draw(disk).ellipse((CX - R + 30*S, CY - R + 30*S, CX + R - 30*S, CY + R - 30*S), fill=255)
layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
layer.paste(head, (CX - head.width // 2, CY - int(head.height * 0.56)))
white = Image.new("RGBA", (W, H), WHITE)
out.paste(white, (0, 0), disk)
out.paste(layer, (0, 0), Image.composite(layer.split()[3], Image.new("L", (W, H), 0), disk))

d = ImageDraw.Draw(out)
# Ring.
d.ellipse((CX - R, CY - R, CX + R, CY + R), outline=CYAN, width=34 * S)

# Bottom: the log, with records.
bar_y = 925 * S
bar_h = 34 * S
x0, x1 = 60 * S, 940 * S
d.rounded_rectangle((x0, bar_y, x1, bar_y + bar_h), radius=10 * S, fill=INK)
cell = (x1 - x0 - 16 * S) / 22
for i in range(22):
    cx = x0 + 8 * S + i * cell
    col = CYAN if i % 3 else CYAN_L
    if i == 15: col = AMBER
    d.rectangle((cx + 3 * S, bar_y + 8 * S, cx + cell - 3 * S, bar_y + bar_h - 8 * S), fill=col)

# Three nodes on the log. The left one is down; its workload hops to the middle one.
DEAD = (120, 128, 140, 255)
nodes = [(230, "dead"), (500, "live"), (770, "ok")]
nw, nh, ny = 190 * S, 150 * S, 760 * S
for nx, state in nodes:
    nx *= S
    d.line((nx, ny + nh // 2, nx, bar_y), fill=INK, width=14 * S)
for nx, state in nodes:
    nx *= S
    body = DEAD if state == "dead" else INK
    rim = AMBER if state == "live" else (DEAD if state == "dead" else CYAN)
    l, t = nx - nw // 2, ny - nh // 2
    d.rounded_rectangle((l - 9*S, t - 9*S, l + nw + 9*S, t + nh + 9*S), radius=30 * S, fill=WHITE)
    d.rounded_rectangle((l, t, l + nw, t + nh), radius=22 * S, fill=body, outline=rim, width=10 * S)
    # Two rack units, each with a slot and a status light.
    for k in range(2):
        uy = t + 40 * S + k * 62 * S
        slot = (70, 76, 88, 255) if state == "dead" else (44, 50, 62, 255)
        d.rounded_rectangle((l + 28*S, uy - 9*S, l + 120*S, uy + 9*S), radius=5 * S, fill=slot)
        led = (150, 156, 166, 255) if state == "dead" else (AMBER if state == "live" else CYAN_L)
        d.ellipse((l + 142*S, uy - 13*S, l + 168*S, uy + 13*S), fill=led)
    if state == "dead":
        for a, b in (((l + 50*S, t + 30*S), (l + nw - 50*S, t + nh - 30*S)), ((l + nw - 50*S, t + 30*S), (l + 50*S, t + nh - 30*S))):
            d.line((*a, *b), fill=WHITE, width=30 * S)
            d.line((*a, *b), fill=(220, 60, 60, 255), width=18 * S)

# The hop: an arc from the dead node to the live one, with a head.
ax0, ax1, top = 230 * S, 500 * S, 610 * S
box = (ax0, top, ax1, ny + 60 * S)
d.arc(box, 200, 328, fill=WHITE, width=42 * S)
d.arc(box, 200, 328, fill=AMBER, width=26 * S)
# Arrowhead along the arc's tangent at its end.
cx, cy, rx, ry = (ax0 + ax1) / 2, (top + ny + 60 * S) / 2, (ax1 - ax0) / 2, (ny + 60 * S - top) / 2
t = math.radians(328)
px, py = cx + rx * math.cos(t), cy + ry * math.sin(t)
dx, dy = -rx * math.sin(t), ry * math.cos(t)
n = math.hypot(dx, dy); dx, dy = dx / n, dy / n
tip = (px + dx * 50 * S, py + dy * 50 * S)
b1 = (px - dy * 34 * S, py + dx * 34 * S)
b2 = (px + dy * 34 * S, py - dx * 34 * S)
d.polygon([tip, b1, b2], fill=AMBER)

# Small compass ticks like the siblings.
for ang in (0, 90, 270):
    a = math.radians(ang - 90)
    rx, ry = CX + math.cos(a) * (R + 45 * S), CY + math.sin(a) * (R + 45 * S)
    d.line((CX + math.cos(a) * (R + 30*S), CY + math.sin(a) * (R + 30*S), rx, ry), fill=CYAN, width=14 * S)

out = out.resize((W // S, H // S), Image.LANCZOS)
out = out.crop(out.getbbox())
out.save(sys.argv[2])

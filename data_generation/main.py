import random
import csv


def generate_data_points(num_points):
    """Generate n 2-dimensional points"""
    result = []
    for _ in range(num_points):
        x = random.randint(0, 10000)
        y = random.randint(0, 10000)
        result.append((x, y))
    return result


def write_to_csv(filename, points):
    """ Write points to a CSV file. """
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        for point in points:
            writer.writerow(point)


if __name__ == '__main__':
    data_points = generate_data_points(5000)
    write_to_csv('datasets/data_points.csv', data_points)

    # generate K seed points
    K = int(input("Enter the value of K: "))
    seed_points = generate_data_points(K)
    write_to_csv(f"datasets/{K}seed_points.csv", seed_points)